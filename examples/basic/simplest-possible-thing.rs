use cqrs::{
    aggregate,
    di::{CqrsExt, InMemoryExt},
    encoding::Uuid,
    event, transcode, when, Aggregate, Repository,
};
use di::ServiceCollection;
use prost::Message;
use std::error::Error;

// #[transcode] will:
// 1. erase 'mod events'; this is required because Rust doesn't currently
//    support 'inner' macros (e.g. #![transcode])
// 2. create a public module named 'transcoder'
// 3. create a public factory function named 'events' in the 'transcoder' module
//    which returns Transcoder<dyn Event> for all of the defined events
// 4. create a public factory function named 'snapshots' in the 'transcoder' module
//    which returns Transcoder<dyn Snapshot> for all of the defined snapshots, if any
//
// this example uses ProtoBuf (via prost) for message encoding, but the following
// other formats are supported behind features:
// - JSON ("json")
// - CBOR ("cbor")
// - Message Pack ("message-pack")
#[transcode(with = cqrs::encoding::ProtoBuf)]
mod events {
    #[event]
    #[derive(Message)]
    pub struct Created {
        #[prost(message)]
        id: Option<Uuid>,

        #[prost(string)]
        name: String,
    }

    impl Created {
        pub fn id(&self) -> Uuid {
            self.id.unwrap_or_default()
        }

        pub fn name(&self) -> &str {
            &self.name
        }

        pub fn new(id: Uuid, name: String) -> Self {
            Self {
                id: Some(id),
                version: Default::default(),
                name,
            }
        }
    }

    #[event]
    #[derive(Message)]
    pub struct Deactivated {
        #[prost(message)]
        id: Option<Uuid>,
    }

    impl Deactivated {
        pub fn id(&self) -> Uuid {
            self.id.unwrap_or_default()
        }

        pub fn new(id: Uuid) -> Self {
            Self {
                id: Some(id),
                version: Default::default(),
            }
        }
    }

    #[event]
    #[derive(Message)]
    pub struct Renamed {
        #[prost(message)]
        id: Option<Uuid>,

        #[prost(string)]
        new_name: String,
    }

    impl Renamed {
        pub fn id(&self) -> Uuid {
            self.id.unwrap_or_default()
        }

        pub fn new_name(&self) -> &str {
            &self.new_name
        }

        pub fn new(id: Uuid, new_name: String) -> Self {
            Self {
                id: Some(id),
                version: Default::default(),
                new_name,
            }
        }
    }
}

// #[aggregate] adds the 'id' field; if not otherwise specified,
// the default type is Uuid
#[aggregate]
#[derive(Default)]
pub struct InventoryItem {
    active: bool,
    name: String,
}

// #[aggregate] here implements the Aggregate trait.
// #[when] identifies the function to call when an event is replayed
#[aggregate]
impl InventoryItem {
    pub fn is_active(&self) -> bool {
        self.active
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn create<S: AsRef<str>>(id: Uuid, name: S) -> Self {
        let mut me = Self::default();
        me.record(Created::new(id, name.as_ref().into()));
        me
    }

    pub fn deactivate(&mut self) {
        if self.active {
            self.record(Deactivated::new(self.id.into()));
        }
    }

    pub fn rename<S: AsRef<str>>(&mut self, new_name: S) {
        if self.name != new_name.as_ref() {
            self.record(Renamed::new(self.id.into(), new_name.as_ref().into()));
        }
    }

    #[when]
    fn _created(&mut self, event: &Created) {
        self.id = event.id().into();
        self.active = true;
        self.name = event.name().into();
    }

    #[when]
    fn _deactivated(&mut self, _event: &Deactivated) {
        self.active = false;
    }

    #[when]
    fn _renamed(&mut self, event: &Renamed) {
        self.name = event.new_name().into();
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + 'static>> {
    // add CQRS to DI, which adds the following services:
    // - Transcoder<dyn Event>>
    // - Transcoder<dyn Snapshot>>
    // - event::Store<Uuid>> with key InventoryItem
    // - snapshot::Store<Uuid>> with key InventoryItem
    //   - only if .in_memory().with().snapshots() is called
    // - Repository<InventoryItem>
    let provider = ServiceCollection::new()
        .add_cqrs(|options| {
            // there's no reflection or automatic transcoder discovery mechanism. register
            // all transcoders here. this function was generated by #[transcode] above
            options.transcoders.events.push(self::transcoder::events());

            // indicate the storage configuration for aggregates, optionally with snapshots
            options.store::<InventoryItem>().in_memory();
            // options.store::<InventoryItem>().in_memory().with().snapshots();
        })
        .build_provider()
        .unwrap();
    let repository = provider.get_required::<Repository<InventoryItem>>();

    // 1. create a new id; prost doesn't provide native support for encoding a uuid so an surrogate is used
    let id: Uuid = uuid::Uuid::new_v4().into();

    // 2. create an item with a misspelled name
    let mut item = InventoryItem::create(id, "Exemple");
    repository.save(&mut item).await?;

    // 3. rename the item
    item.rename("Example");
    repository.save(&mut item).await?;

    // 4. deactivate the item
    item.deactivate();
    repository.save(&mut item).await?;

    // 5. reify the item from storage via the repository
    item = repository.get(&id, None).await?;

    // 6. display what we have
    println!(
        r#"The item "{}" is {} (id: {})"#,
        item.name(),
        match item.is_active() {
            true => "active",
            false => "inactive",
        },
        item.id().as_hyphenated(),
    );
    println!(
        "Versions are opaque to us, but storage is tracking it as {}",
        item.version()
    );
    Ok(())
}
