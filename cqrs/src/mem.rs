use crate::{
    event::{self, Event, EventStream, IdStream, Predicate, StoreError},
    message::{Descriptor, Schema, Transcoder},
    snapshot::{self, Snapshot, SnapshotError},
    Clock, Range, Version,
};
use async_trait::async_trait;
use cfg_if::cfg_if;
use futures::stream;
use std::{
    collections::HashMap,
    fmt::Debug,
    hash::Hash,
    ops::Bound::{self, Excluded, Included},
    sync::{Arc, RwLock},
    time::SystemTime,
};
use uuid::Uuid;

// version encoding
//
// | 32-bits | 24-bits | 8-bits   |
// | ------- | ------- | -------- |
// | version | unused  | sequence |

trait EncodedVersion: Sized {
    fn max() -> Self;
    fn number(&self) -> u32;
    fn sequence(&self) -> u8;
    fn next_version(&self) -> Self;
    fn next_sequence(&self) -> Self;
}

impl EncodedVersion for Version {
    fn max() -> Self {
        Self::new((u32::MAX as u64) << 32 | (u8::MAX as u64))
    }

    fn number(&self) -> u32 {
        (u64::from(self) >> 32) as u32
    }

    fn sequence(&self) -> u8 {
        (u64::from(self) & 0x00000000_000000FF) as u8
    }

    fn next_version(&self) -> Self {
        Self::new((self.number().saturating_add(1) as u64) << 32)
    }

    fn next_sequence(&self) -> Self {
        let version = self.number();
        let sequence = self.sequence().saturating_add(1);
        Self::new((version as u64) << 32 | sequence as u64)
    }
}

struct Transaction<'a> {
    state: Option<&'a mut [Box<dyn Event>]>,
    versions: Vec<Version>,
}

impl<'a> Transaction<'a> {
    fn begin(events: &'a mut [Box<dyn Event>]) -> Self {
        let mut versions = Vec::with_capacity(events.len());

        for event in &*events {
            versions.push(event.version());
        }

        Self {
            state: Some(events),
            versions,
        }
    }

    fn events(&mut self) -> &mut [Box<dyn Event>] {
        self.state.as_deref_mut().unwrap()
    }

    fn commit(&mut self) {
        self.state = None;
    }
}

impl Drop for Transaction<'_> {
    fn drop(&mut self) {
        if let Some(events) = self.state.take() {
            for (i, event) in events.iter_mut().enumerate() {
                event.set_version(self.versions[i]);
            }
        }
    }
}

#[derive(Clone)]
struct Row {
    schema: Schema,
    version: Version,
    data: Vec<u8>,
}

/// Represents an in-memory [snapshot store](snapshot::Store).
pub struct SnapshotStore<T = Uuid> {
    table: RwLock<HashMap<T, Row>>,
    transcoder: Arc<Transcoder<dyn Snapshot>>,
}

impl<T> SnapshotStore<T> {
    /// Initializes a new in-memory [`SnapshotStore`].
    ///
    /// # Arguments
    ///
    /// * `transcoder` - the [transcoder](Transcoder) uses to encode and decode snapshots
    pub fn new(transcoder: Transcoder<dyn Snapshot>) -> Self {
        Self {
            table: Default::default(),
            transcoder: transcoder.into(),
        }
    }
}

#[async_trait]
impl<T: Clone + Debug + Eq + Hash + Send + Sync> snapshot::Store<T> for SnapshotStore<T> {
    async fn load(
        &self,
        id: &T,
        predicate: Option<&snapshot::Predicate>,
    ) -> Result<Option<Box<dyn Snapshot>>, SnapshotError> {
        if let Some(descriptor) = self.load_raw(id, predicate).await? {
            Ok(Some(
                self.transcoder
                    .decode(&descriptor.schema, &descriptor.content)
                    .map_err(SnapshotError::InvalidEncoding)?,
            ))
        } else {
            Ok(None)
        }
    }

    async fn load_raw(
        &self,
        id: &T,
        predicate: Option<&snapshot::Predicate>,
    ) -> Result<Option<Descriptor>, SnapshotError> {
        let table = self.table.read().unwrap();

        if let Some(row) = table.get(id) {
            if let Some(predicate) = predicate {
                match predicate.min_version {
                    Included(version) => {
                        if row.version >= version {
                            return Ok(None);
                        }
                    }
                    Excluded(version) => {
                        if row.version > version {
                            return Ok(None);
                        }
                    }
                    _ => {}
                }
            }

            return Ok(Some(Descriptor::new(
                row.schema.clone(),
                row.version,
                row.data.clone(),
            )));
        }

        Ok(None)
    }

    async fn save(&self, id: &T, snapshot: Box<dyn Snapshot>) -> Result<(), SnapshotError> {
        let mut table = self.table.write().unwrap();

        if let Some(row) = table.get(id) {
            let current = self.transcoder.decode(&row.schema, &row.data)?;

            if snapshot.version() <= current.version() {
                return Ok(());
            }
        }

        let row = Row {
            schema: snapshot.schema(),
            version: snapshot.version(),
            data: self
                .transcoder
                .encode(&*snapshot)
                .map_err(SnapshotError::InvalidEncoding)?,
        };
        let _ = table.insert(id.clone(), row);

        Ok(())
    }
}

/// Represents an in-memory [event store](event::Store).
pub struct EventStore<T = Uuid> {
    table: RwLock<HashMap<T, Vec<Vec<Row>>>>,
    transcoder: Arc<Transcoder<dyn Event>>,
    clock: Arc<dyn Clock>,
    snapshots: Option<Arc<dyn snapshot::Store<T>>>,
}

impl<T> EventStore<T> {
    /// Initializes a new in-memory [`EventStore`].
    ///
    /// # Arguments
    ///
    /// * `clock` - the type of [clock](Clock)
    /// * `transcoder` - the [transcoder](Transcoder) uses to encode and decode events
    pub fn new<C>(clock: C, transcoder: Transcoder<dyn Event>) -> Self
    where
        C: Clock + 'static,
    {
        Self {
            table: Default::default(),
            transcoder: transcoder.into(),
            clock: Arc::new(clock),
            snapshots: Default::default(),
        }
    }

    /// Gets the size of the store.
    pub fn size(&self) -> usize {
        self.table
            .read()
            .unwrap()
            .values()
            .flatten()
            .flatten()
            .count()
    }
}

impl<T: Clone + Debug + Eq + Hash + Send + Sync> EventStore<T> {
    /// Initializes a new in-memory [`EventStore`] with support for [snapshots](Snapshot).
    ///
    /// # Arguments
    ///
    /// * `clock` - the type of [clock](Clock)
    /// * `transcoder` - the [transcoder](Transcoder) uses to encode and decode events
    /// * `snapshots` - the [store](SnapshotStore) used for [snapshots](Snapshot)
    pub fn with_snapshots<C>(
        clock: C,
        transcoder: Transcoder<dyn Event>,
        snapshots: Arc<dyn snapshot::Store<T>>,
    ) -> Self
    where
        C: Clock + 'static,
    {
        Self {
            table: Default::default(),
            transcoder: transcoder.into(),
            clock: Arc::new(clock),
            snapshots: Some(snapshots),
        }
    }

    async fn get_snapshot(
        &self,
        predicate: Option<&Predicate<'_, T>>,
    ) -> Result<Option<Descriptor>, SnapshotError> {
        if let Some(snapshots) = self.snapshots.as_deref() {
            if let Some(predicate) = predicate {
                if predicate.load.snapshots {
                    if let Some(id) = predicate.id {
                        let predicate = Some(predicate.into());
                        return snapshots.load_raw(id, predicate.as_ref()).await;
                    }
                }
            }
        }

        Ok(None)
    }
}

#[inline]
fn greater_than_or_equal(bound: &Bound<SystemTime>, now: &SystemTime) -> bool {
    match bound {
        Included(value) => now >= value,
        Excluded(value) => now > value,
        _ => true,
    }
}

#[inline]
fn less_than_or_equal(bound: &Bound<SystemTime>, now: &SystemTime) -> bool {
    match bound {
        Included(value) => now <= value,
        Excluded(value) => now < value,
        _ => true,
    }
}

fn by<T: Debug + Send>(row: &Row, now: SystemTime, option: Option<&Predicate<T>>) -> bool {
    if let Some(predicate) = option {
        if predicate.types.is_empty() || predicate.types.contains(&row.schema) {
            greater_than_or_equal(&predicate.stored_on.from, &now)
                && less_than_or_equal(&predicate.stored_on.to, &now)
        } else {
            false
        }
    } else {
        true
    }
}

fn select_version<T: Debug + Send>(
    snapshot: Option<&Descriptor>,
    predicate: &Predicate<'_, T>,
) -> Option<usize> {
    if let Some(snapshot) = snapshot {
        let version = snapshot.version;

        match predicate.version {
            Included(other) => {
                if other >= version {
                    return Some(other.number() as usize);
                }
            }
            Excluded(other) => {
                if other > version {
                    return Some(other.number() as usize);
                }
            }
            _ => {}
        }

        Some(version.number() as usize)
    } else {
        match predicate.version {
            Included(version) => Some(version.number() as usize),
            Excluded(version) => Some(version.number() as usize),
            _ => None,
        }
    }
}

#[async_trait]
impl<T: Clone + Debug + Eq + Hash + Send + Sync + 'static> event::Store<T> for EventStore<T> {
    async fn ids(&self, _stored_on: Range<SystemTime>) -> IdStream<T> {
        // TODO: using a date range for an in-memory store requires additional thinking as the setup
        // would be contrived with a virtual clock or have timestamps very, very close to each other.
        let table = self.table.read().unwrap();
        let ids: Vec<_> = table.keys().cloned().map(Ok).collect();

        Box::pin(stream::iter(ids.into_iter()))
    }

    async fn load<'a>(&self, predicate: Option<&'a Predicate<'a, T>>) -> EventStream<'a, T> {
        let snapshot = match self.get_snapshot(predicate).await {
            Ok(snapshot) => snapshot,
            Err(error) => return Box::pin(stream::iter(vec![Err(StoreError::from(error))])),
        };
        let table = self.table.read().unwrap();
        let option = Arc::new(predicate);

        if let Some(predicate) = option.as_deref() {
            if let Some(id) = predicate.id {
                if let Some(rows) = table.get(id) {
                    let transcoder = self.transcoder.clone();
                    let now = self.clock.clone().now();
                    let mut rows = if let Some(index) = select_version(snapshot.as_ref(), predicate)
                    {
                        rows.iter().skip(index).cloned().collect()
                    } else {
                        rows.clone()
                    };

                    if let Some(snapshot) = snapshot {
                        rows.insert(
                            0,
                            vec![Row {
                                schema: snapshot.schema,
                                version: snapshot.version,
                                data: snapshot.content,
                            }],
                        );
                    }

                    Box::pin(stream::iter(
                        rows.into_iter()
                            .flatten()
                            .filter(move |row| by(row, now, option.as_deref()))
                            .map(move |row| {
                                transcoder
                                    .decode(&row.schema, &row.data)
                                    .map_err(StoreError::InvalidEncoding)
                            }),
                    ))
                } else {
                    Box::pin(futures::stream::iter(std::iter::empty()))
                }
            } else {
                let transcoder = self.transcoder.clone();
                let now = self.clock.clone().now();
                let version = match predicate.version {
                    Included(version) => Some(version),
                    Excluded(version) => Some(version),
                    _ => None,
                };
                let rows: Vec<_> = if let Some(version) = version {
                    let index = version.number() as usize;
                    let mut filtered = Vec::new();

                    for rows in table.values() {
                        filtered.extend(rows.iter().skip(index).cloned());
                    }

                    filtered
                } else {
                    table.values().flatten().cloned().collect()
                };

                Box::pin(stream::iter(
                    rows.into_iter()
                        .flatten()
                        .filter(move |row| by(row, now, option.as_deref()))
                        .map(move |row| {
                            transcoder
                                .decode(&row.schema, &row.data)
                                .map_err(StoreError::InvalidEncoding)
                        }),
                ))
            }
        } else {
            let transcoder = self.transcoder.clone();
            let rows: Vec<_> = table.values().flatten().flatten().cloned().collect();

            Box::pin(stream::iter(rows.into_iter().map(move |row| {
                transcoder
                    .decode(&row.schema, &row.data)
                    .map_err(StoreError::InvalidEncoding)
            })))
        }
    }

    async fn save(
        &self,
        id: &T,
        events: &mut [Box<dyn Event>],
        expected_version: Version,
    ) -> Result<(), StoreError<T>> {
        if events.len() > <Version as EncodedVersion>::max().sequence() as usize {
            return Err(StoreError::BatchTooLarge(
                <Version as EncodedVersion>::max().sequence(),
            ));
        }

        let mut table = self.table.write().unwrap();

        if let Some(rows) = table.get(id) {
            let count = rows.len();

            if count > 0 && count > expected_version.number() as usize {
                return Err(StoreError::Conflict(id.clone(), expected_version.number()));
            }
        }

        let mut version = expected_version.next_version();
        let mut rows = Vec::new();
        let mut tx = Transaction::begin(events);

        for event in &mut *tx.events() {
            event.set_version(version);

            let row = Row {
                schema: event.schema(),
                version,
                data: self
                    .transcoder
                    .encode(event.as_ref())
                    .map_err(StoreError::InvalidEncoding)?,
            };

            rows.push(row);
            version = version.next_sequence();
        }

        let mut rows = vec![rows];

        table
            .entry(id.clone())
            .and_modify(|row| row.append(&mut rows))
            .or_insert(rows);
        tx.commit();

        Ok(())
    }
}

cfg_if! {
    if #[cfg(feature = "di")] {
        use di::{inject, injectable, Ref};

        #[injectable(snapshot::Store<T>)]
        impl<T: Clone + Debug + Eq + Hash + Send + Sync + 'static> SnapshotStore<T> {
            #[inject]
            fn _new(transcoder: Ref<Transcoder<dyn Snapshot>>) -> Self {
                Self {
                    table: Default::default(),
                    transcoder,
                }
            }
        }

        #[injectable(event::Store<T>)]
        impl<T: Clone + Debug + Eq + Hash + Send + Sync + 'static> EventStore<T> {
            #[inject]
            fn _new(
                clock: Ref<dyn Clock>,
                transcoder: Ref<Transcoder<dyn Event>>,
                snapshots: Option<Arc<dyn snapshot::Store<T>>>,
            ) -> Self {
                Self {
                    table: Default::default(),
                    transcoder,
                    clock,
                    snapshots,
                }
            }
        }
    }
}
