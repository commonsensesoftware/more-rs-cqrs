use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

/// Defines the behavior of a wall clock.
pub trait Clock: Send + Sync {
    /// Gets the clock's current [date and time](SystemTime).
    fn now(&self) -> SystemTime;
}

/// Represents a wall [clock](Clock).
#[cfg_attr(feature = "di", di::injectable(Clock))]
#[derive(Copy, Clone, Default)]
pub struct WallClock;

impl WallClock {
    /// Initializes a new [`WallClock`].
    pub fn new() -> Self {
        Self
    }
}

impl Clock for WallClock {
    fn now(&self) -> SystemTime {
        SystemTime::now()
    }
}

/// Represents a virtual [clock](Clock).
#[derive(Clone)]
pub struct VirtualClock(Arc<dyn Fn() -> SystemTime + Send + Sync>);

impl VirtualClock {
    /// Initializes a new [`VirtualClock`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Resets the clock.
    pub fn reset(&mut self) {
        self.0 = Arc::new(SystemTime::now);
    }

    /// Sets the clock to a specific date and time.
    ///
    /// # Arguments
    ///
    /// * `when` - the [date and time](SystemTime) to set the clock to
    pub fn set(&mut self, when: SystemTime) {
        let then = SystemTime::now();
        self.0 = Arc::new(move || when + SystemTime::now().duration_since(then).unwrap());
    }

    /// Winds the clock forward by the specific amount of time.
    ///
    /// # Arguments
    ///
    /// * `time` - the [amount of time](Duration) to wind the clock by
    pub fn wind(&mut self, time: Duration) {
        let then = self.0.clone();
        self.0 = Arc::new(move || (then)() + time);
    }

    /// Rewinds the clock backward by the specific amount of time.
    ///
    /// # Arguments
    ///
    /// * `time` - the [amount of time](Duration) to rewind the clock by
    pub fn rewind(&mut self, time: Duration) {
        let then = self.0.clone();
        self.0 = Arc::new(move || (then)() - time);
    }
}

impl Default for VirtualClock {
    fn default() -> Self {
        Self(Arc::new(SystemTime::now))
    }
}

impl Clock for VirtualClock {
    fn now(&self) -> SystemTime {
        (self.0)()
    }
}

impl From<SystemTime> for VirtualClock {
    fn from(value: SystemTime) -> Self {
        Self(Arc::new(move || value))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn winding_virtual_clock_should_report_expected_time() {
        // arrange
        let now = SystemTime::now();
        let mut clock: VirtualClock = now.clone().into();
        let five_mins = Duration::from_secs(60 * 5);

        // act
        clock.wind(five_mins);

        // assert
        assert_eq!(clock.now(), now + five_mins);
    }

    #[test]
    fn rewinding_virtual_clock_should_report_expected_time() {
        // arrange
        let now = SystemTime::now();
        let mut clock: VirtualClock = now.clone().into();
        let five_mins = Duration::from_secs(60 * 5);

        // act
        clock.rewind(five_mins);

        // assert
        assert_eq!(clock.now(), now - five_mins);
    }

    #[test]
    fn resetting_virtual_clock_should_report_expected_time() {
        // arrange
        let mut clock = VirtualClock::new();
        let five_mins = Duration::from_secs(60 * 5);

        clock.wind(five_mins);

        // act
        clock.reset();
        
        let then = SystemTime::now();
        let elapsed = loop {
            if let Ok(interval) = clock.now().duration_since(then) {
                break interval.as_millis();
            }
        };

        // assert
        assert!(elapsed <= 1, "expected <= 1ms, but observed {elapsed}ms");
    }

    #[test]
    fn winding_and_rewinding_virtual_clock_should_cancel_each_other() {
        // arrange
        let now = SystemTime::now();
        let mut clock: VirtualClock = now.clone().into();
        let five_mins = Duration::from_secs(60 * 5);

        // act
        clock.wind(five_mins);
        clock.rewind(five_mins);

        // assert
        assert_eq!(clock.now(), now);
    }

    #[test]
    fn setting_virtual_clock_should_store_expected_epoch() {
        // arrange
        let mut clock = VirtualClock::new();
        let yesterday = SystemTime::now() - Duration::from_secs(60 * 60 * 24);

        // act
        clock.set(yesterday);
        sleep(Duration::from_millis(250));
        let elapsed = SystemTime::now()
            .duration_since(clock.now())
            .unwrap()
            .as_millis();

        // assert
        assert!(elapsed > 0 && elapsed < 86400250);
    }
}
