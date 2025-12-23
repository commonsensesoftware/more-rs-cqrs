use serde::{
    Deserialize, Deserializer,
    de::{self, MapAccess, Visitor},
};
use sqlx::{Database, pool::PoolOptions};
use std::{
    fmt::{Formatter, Result as FormatResult},
    time::Duration,
};

const FIELDS_PASCAL: &[&str; 6] = &[
    "Url",
    "MaxConnections",
    "MinConnections",
    "ConnectTimeout",
    "MaxLifetime",
    "IdleTimeout",
];

const FIELDS_SNAKE: &[&str; 6] = &[
    "url",
    "max_connections",
    "min_connections",
    "connect_timeout",
    "max_lifetime",
    "idle_timeout",
];

#[derive(Copy, Clone, Debug)]
enum Field {
    Url,
    MaxConnections,
    MinConnections,
    ConnectTimeout,
    MaxLifetime,
    IdleTimeout,
}

impl Field {
    fn as_str(&self) -> &'static str {
        FIELDS_PASCAL[*self as usize]
    }
}

impl TryFrom<&str> for Field {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let fields = [
            Field::Url,
            Field::MaxConnections,
            Field::MinConnections,
            Field::ConnectTimeout,
            Field::MaxLifetime,
            Field::IdleTimeout,
        ];

        for field in fields {
            let name = field.as_str();

            if name == value
                || name.eq_ignore_ascii_case(value)
                || FIELDS_SNAKE[field as usize] == value
            {
                return Ok(field);
            }
        }

        Err(())
    }
}

/// Represents SQL storage options.
pub struct SqlOptions<DB: Database> {
    /// Gets or sets the URL representing the database connection string.
    pub url: String,

    /// Gets or sets the [connection pool options](PoolOptions).
    pub options: PoolOptions<DB>,
}

impl<DB: Database> Default for SqlOptions<DB> {
    fn default() -> Self {
        Self {
            url: Default::default(),
            options: Default::default(),
        }
    }
}

impl<'de, DB: Database> Deserialize<'de> for SqlOptions<DB> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut place = Self::default();
        Self::deserialize_in_place(deserializer, &mut place)?;
        Ok(place)
    }

    fn deserialize_in_place<D>(deserializer: D, place: &mut Self) -> Result<(), D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_struct("SqlOptions", FIELDS_PASCAL, InPlaceVisitor(place))
    }
}

impl<'de> Deserialize<'de> for Field {
    fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FieldVisitor;

        impl<'de> Visitor<'de> for FieldVisitor {
            type Value = Field;

            fn expecting(&self, formatter: &mut Formatter) -> FormatResult {
                formatter.write_fmt(format_args!(
                    "one of: {}",
                    [*FIELDS_PASCAL, *FIELDS_SNAKE].concat().join(",")
                ))
            }

            fn visit_str<E>(self, value: &str) -> Result<Field, E>
            where
                E: de::Error,
            {
                Field::try_from(value).map_err(|_| de::Error::unknown_field(value, FIELDS_PASCAL))
            }
        }

        deserializer.deserialize_identifier(FieldVisitor)
    }
}

struct InPlaceVisitor<'a, DB: Database + 'a>(&'a mut SqlOptions<DB>);

impl<'a, 'de, DB: Database> Visitor<'de> for InPlaceVisitor<'a, DB> {
    type Value = ();

    fn expecting(&self, formatter: &mut Formatter) -> FormatResult {
        formatter.write_str("struct SqlOptions")
    }

    fn visit_map<V>(self, mut map: V) -> Result<Self::Value, V::Error>
    where
        V: MapAccess<'de>,
    {
        while let Some(field) = map.next_key()? {
            match field {
                Field::Url => {
                    if self.0.url.is_empty() {
                        self.0.url = map.next_value()?;
                    } else {
                        return Err(de::Error::duplicate_field(field.as_str()));
                    }
                }
                Field::MinConnections | Field::MaxConnections => {
                    let num: u32 = map.next_value()?;

                    match field {
                        Field::MinConnections => {
                            self.0.options = self.0.options.clone().min_connections(num);
                        }
                        Field::MaxConnections => {
                            self.0.options = self.0.options.clone().max_connections(num);
                        }
                        _ => unreachable!(),
                    }
                }
                _ => {
                    let secs: u64 = map.next_value()?;
                    let duration = Duration::from_secs(secs);

                    match field {
                        Field::ConnectTimeout => {
                            self.0.options = self.0.options.clone().acquire_timeout(duration);
                        }
                        Field::IdleTimeout => {
                            self.0.options = self.0.options.clone().idle_timeout(duration);
                        }
                        Field::MaxLifetime => {
                            self.0.options = self.0.options.clone().max_lifetime(duration);
                        }
                        _ => unreachable!(),
                    }
                }
            }
        }

        Ok(())
    }
}
