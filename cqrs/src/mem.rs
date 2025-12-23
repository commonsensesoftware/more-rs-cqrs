use crate::{
    Clock, Mask, Range, Version,
    event::{self, Event, EventStream, IdStream, Predicate, StoreError},
    message::{Descriptor, Saved, Schema},
    snapshot::{self, Retention, Snapshot, SnapshotError},
};
use async_trait::async_trait;
use futures::{
    future::ready,
    stream::{self, once},
};
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

fn new_version(number: u32) -> Version {
    Version::new((number as u64) << 32)
}

trait EncodedVersion: Sized {
    fn max() -> Self;
    fn number(&self) -> u32;
    fn sequence(&self) -> u8;
    fn next_version(&self) -> Self;
    fn next_sequence(&self) -> Self;
    fn invalid(&self) -> bool;
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

    fn invalid(&self) -> bool {
        (u64::from(self) & 0x00000000_FFFFFF00) != 0
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
    options: snapshot::StoreOptions,
}

impl<T> SnapshotStore<T> {
    /// Initializes a new in-memory [SnapshotStore].
    ///
    /// # Arguments
    ///
    /// * `options` - the [store options](snapshot::StoreOptions)
    pub fn new(options: snapshot::StoreOptions) -> Self {
        Self {
            table: Default::default(),
            options,
        }
    }
}

impl<T: Clone + Debug + Eq + Hash + Send + Sync + 'static> From<SnapshotStore<T>> for Arc<dyn snapshot::Store<T>> {
    fn from(value: SnapshotStore<T>) -> Self {
        Arc::new(value)
    }
}

#[async_trait]
impl<T: Clone + Debug + Eq + Hash + Send + Sync> snapshot::Store<T> for SnapshotStore<T> {
    async fn load(
        &self,
        id: &T,
        predicate: Option<&snapshot::Predicate>,
    ) -> Result<Option<Saved<Box<dyn Snapshot>>>, SnapshotError> {
        if let Some(descriptor) = self.load_raw(id, predicate).await? {
            Ok(Some(Saved::new(
                self.options
                    .transcoder()
                    .decode(&descriptor.schema, &descriptor.content)
                    .map_err(SnapshotError::InvalidEncoding)?,
                descriptor.version,
            )))
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

    async fn save(
        &self,
        id: &T,
        mut version: Version,
        snapshot: Box<dyn Snapshot>,
    ) -> Result<(), SnapshotError> {
        if version != Default::default()
            && let Some(mask) = self.options.mask()
        {
            version = version.unmask(mask);
        }

        if version.invalid() {
            return Err(SnapshotError::InvalidVersion);
        }

        let mut table = self.table.write().unwrap();

        if let Some(row) = table.get(id)
            && version <= row.version
        {
            return Ok(());
        }

        let row = Row {
            schema: snapshot.schema(),
            version,
            data: self
                .options
                .transcoder()
                .encode(&*snapshot)
                .map_err(SnapshotError::InvalidEncoding)?,
        };
        let _ = table.insert(id.clone(), row);

        Ok(())
    }

    async fn prune(&self, id: &T, _retention: Option<&Retention>) -> Result<(), SnapshotError> {
        let _ = self.table.write().unwrap().remove(id);
        Ok(())
    }
}

/// Represents an in-memory [event store](event::Store).
pub struct EventStore<ID = Uuid> {
    table: RwLock<HashMap<ID, Vec<Vec<Row>>>>,
    options: event::StoreOptions<ID>,
}

impl<ID: Clone + Debug + Eq + Hash + Send + Sync> EventStore<ID> {
    /// Initializes a new in-memory [EventStore].
    ///
    /// # Arguments
    ///
    /// * `options` - the [store options](event::StoreOptions)
    pub fn new(options: event::StoreOptions<ID>) -> Self {
        Self {
            table: Default::default(),
            options,
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

    async fn get_snapshot(
        &self,
        predicate: Option<&Predicate<'_, ID>>,
    ) -> Result<Option<Descriptor>, SnapshotError> {
        if let Some(snapshots) = self.options.snapshots().as_deref()
            && let Some(predicate) = predicate
            && predicate.load.snapshots
            && let Some(id) = predicate.id
        {
            let predicate = Some(predicate.into());
            return snapshots.load_raw(id, predicate.as_ref()).await;
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
    mask: Option<&(dyn Mask + 'static)>,
) -> Option<usize> {
    if let Some(snapshot) = snapshot {
        let mut version = snapshot.version;

        if let Some(mask) = mask {
            version = version.unmask(mask);
        }

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
    fn clock(&self) -> Arc<dyn Clock> {
        (&self.options).into()
    }

    async fn ids(&self, _stored_on: Range<SystemTime>) -> IdStream<T> {
        let table = self.table.read().unwrap();
        let ids: Vec<_> = table.keys().cloned().map(Ok).collect();

        Box::pin(stream::iter(ids))
    }

    async fn load<'a>(&self, predicate: Option<&'a Predicate<'a, T>>) -> EventStream<'a, T> {
        let snapshot = match self.get_snapshot(predicate).await {
            Ok(snapshot) => snapshot,
            Err(error) => {
                return Box::pin(once(ready(Err(StoreError::from(error)))));
            }
        };
        let table = self.table.read().unwrap();
        let option = Arc::new(predicate);

        if let Some(predicate) = option.as_deref() {
            if let Some(id) = predicate.id {
                if let Some(rows) = table.get(id) {
                    let options = self.options.clone();
                    let now = options.clock().now();
                    let mask = options.mask();
                    let mut rows =
                        if let Some(index) = select_version(snapshot.as_ref(), predicate, mask) {
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
                                let mut version = row.version;

                                if let Some(mask) = options.mask() {
                                    version = version.mask(mask);
                                }

                                Ok(Saved::new(
                                    options
                                        .transcoder()
                                        .decode(&row.schema, &row.data)
                                        .map_err(StoreError::InvalidEncoding)?,
                                    version,
                                ))
                            }),
                    ))
                } else {
                    Box::pin(stream::iter(std::iter::empty()))
                }
            } else {
                let options = self.options.clone();
                let now = options.clock().now();
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
                            let mut version = row.version;

                            if let Some(mask) = options.mask() {
                                version = version.mask(mask);
                            }

                            Ok(Saved::new(
                                options
                                    .transcoder()
                                    .decode(&row.schema, &row.data)
                                    .map_err(StoreError::InvalidEncoding)?,
                                version,
                            ))
                        }),
                ))
            }
        } else {
            let options = self.options.clone();
            let rows: Vec<_> = table.values().flatten().flatten().cloned().collect();

            Box::pin(stream::iter(rows.into_iter().map(move |row| {
                let mut version = row.version;

                if let Some(mask) = options.mask() {
                    version = version.mask(mask);
                }

                Ok(Saved::new(
                    options
                        .transcoder()
                        .decode(&row.schema, &row.data)
                        .map_err(StoreError::InvalidEncoding)?,
                    version,
                ))
            })))
        }
    }

    async fn save(
        &self,
        id: &T,
        mut expected_version: Version,
        events: &[Box<dyn Event>],
    ) -> Result<Version, StoreError<T>> {
        if events.is_empty() {
            return Ok(expected_version);
        }

        if expected_version != Version::default()
            && let Some(mask) = self.options.mask()
        {
            expected_version = expected_version.unmask(mask);
        }

        if expected_version.invalid() {
            return Err(StoreError::InvalidVersion);
        }

        if events.len() > <Version as EncodedVersion>::max().sequence() as usize {
            return Err(StoreError::BatchTooLarge(
                <Version as EncodedVersion>::max().sequence(),
            ));
        }

        let mut table = self.table.write().unwrap();

        if let Some(rows) = table.get(id) {
            let count = rows.len();

            if count > 0 && count > expected_version.number() as usize {
                if self.options.concurrency().enforced() {
                    return Err(StoreError::Conflict(id.clone(), expected_version.number()));
                } else {
                    expected_version = new_version(count as u32);
                }
            }
        } else if expected_version.number() > 1 {
            return Err(StoreError::Deleted(id.clone()));
        }

        let mut version = expected_version.next_version();
        let mut rows = Vec::new();

        for event in events {
            let row = Row {
                schema: event.schema(),
                version,
                data: self
                    .options
                    .transcoder()
                    .encode(event.as_ref())
                    .map_err(StoreError::InvalidEncoding)?,
            };

            rows.push(row);
            version = version.next_sequence();
        }

        version = rows.last().unwrap().version;
        let mut rows = vec![rows];

        table
            .entry(id.clone())
            .and_modify(|row| row.append(&mut rows))
            .or_insert(rows);

        if let Some(mask) = self.options.mask() {
            version = version.mask(mask);
        }

        Ok(version)
    }

    async fn delete(&self, id: &T) -> Result<(), StoreError<T>> {
        if self.options.delete().unsupported() {
            return Err(StoreError::Unsupported);
        }

        if let Some(snapshots) = self.options.snapshots() {
            snapshots.prune(id, None).await?;
        }

        let _ = self.table.write().unwrap().remove(id);
        Ok(())
    }
}
