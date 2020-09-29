use slab::Slab;
use std::fmt::{Display, Error, Formatter};
use std::time::Duration;

#[derive(Debug, Hash, PartialEq, PartialOrd, Eq, Ord)]
pub struct TimerId(usize);

impl Display for TimerId {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), Error> {
        write!(f, "TimerId({})", self.0)
    }
}

pub(crate) struct Timers<'a, A, S> {
    index: Vec<TimerId>,
    timers: Slab<Timer<'a, A, S>>,
}

impl<'a, A, S> Timers<'a, A, S> {
    pub(crate) fn new() -> Self {
        Timers {
            index: Vec::new(),
            timers: Slab::new(),
        }
    }

    pub(crate) fn set_oneshot(
        &mut self,
        delay: Duration,
        func: impl FnOnce(&mut A, &mut S, &TimerId) -> TimerId + 'a,
    ) -> TimerId {
        self.set_timer(
            get_system_time() + delay,
            TimerState::OneShot(Box::new(func)),
        )
    }

    pub(crate) fn set_absolute(
        &mut self,
        deadline: Duration,
        func: impl FnOnce(&mut A, &mut S, &TimerId) -> TimerId + 'a,
    ) -> TimerId {
        self.set_timer(deadline, TimerState::OneShot(Box::new(func)))
    }

    pub(crate) fn set_periodic(
        &mut self,
        interval: Duration,
        func: impl FnMut(&mut A, &mut S, &TimerId) + 'a,
    ) -> TimerId {
        self.set_timer(
            get_system_time() + interval,
            TimerState::Periodic(interval, Box::new(func)),
        )
    }

    pub(crate) fn cancel(&mut self, timer_id: TimerId) {
        if let Some(timer) = self.timers.get_mut(timer_id.0) {
            match timer.timer_state {
                TimerState::Calling => timer.timer_state = TimerState::Cancelled,
                TimerState::Cancelled => panic!("Already cancelled timer!"),
                _ => self.remove_timer(timer_id),
            }
        }
    }

    pub(crate) fn delay_to_next_timer(&mut self) -> Option<Duration> {
        if let Some((_, timer)) = self.get_last_timer(None) {
            let current_time = get_system_time();
            if timer.will_fire_at > current_time {
                Some(timer.will_fire_at - current_time)
            } else {
                Some(Duration::from_secs(0))
            }
        } else {
            None
        }
    }

    pub(crate) fn process_timers(
        a: &mut A,
        s: &mut S,
        getter: fn(&mut S) -> &mut Timers<'a, A, S>,
    ) {
        let current_time = get_system_time();
        while let Some((id, timer)) = getter(s).get_last_timer(Some(&current_time)) {
            match std::mem::replace(&mut timer.timer_state, TimerState::Calling) {
                TimerState::OneShot(func) => {
                    let tid = func(a, s, &TimerId(id));
                    assert_eq!(
                        tid.0, id,
                        "Returned TimerId from FnOnce timer is not same as called timer: {} != {}",
                        tid.0, id
                    );
                    getter(s).index.pop().unwrap();
                    getter(s).timers.remove(id);
                }
                TimerState::Periodic(interval, mut func) => {
                    func(a, s, &TimerId(id));
                    getter(s).index.pop().unwrap();
                    let timer = getter(s).timers.get_mut(id).expect("Timer not found");
                    match &mut timer.timer_state {
                        TimerState::Cancelled => {
                            getter(s).timers.remove(id);
                        }
                        TimerState::Calling => {
                            timer.will_fire_at += interval;
                            timer.timer_state = TimerState::Periodic(interval, func);
                            let next_will_fire_at = timer.will_fire_at;
                            getter(s).insert_to_index(TimerId(id), next_will_fire_at);
                        }
                        _ => panic!("Wrong state for timer"),
                    }
                }
                _ => panic!("Wrong state for timer"),
            }
        }
    }

    fn get_last_timer(
        &mut self,
        current_time: Option<&Duration>,
    ) -> Option<(usize, &mut Timer<'a, A, S>)> {
        if let Some(id) = self.index.last() {
            let timer = self.timers.get_mut(id.0).unwrap();
            if let Some(current_time) = current_time {
                if &timer.will_fire_at <= current_time {
                    return Some((id.0, timer));
                }
            } else {
                return Some((id.0, timer));
            }
        }
        None
    }

    fn set_timer(&mut self, will_fire_at: Duration, timer_state: TimerState<'a, A, S>) -> TimerId {
        let entry = self.timers.vacant_entry();
        let timer_id = entry.key();
        entry.insert(Timer {
            will_fire_at,
            timer_state,
        });
        self.insert_to_index(TimerId(timer_id), will_fire_at);
        TimerId(timer_id)
    }

    fn insert_to_index(&mut self, inserted_tid: TimerId, inserted_will_fire_at: Duration) {
        let timers = &self.timers;
        let position = self
            .index
            .binary_search_by(|compared_tid| {
                inserted_will_fire_at.cmp(&timers.get(compared_tid.0).unwrap().will_fire_at)
            })
            .unwrap_err();
        self.index.insert(position, inserted_tid);
    }

    fn remove_from_index(&mut self, removed_tid: TimerId) {
        let removed_will_fire_at = self.timers.get(removed_tid.0).unwrap().will_fire_at;
        let timers = &self.timers;
        let position = self
            .index
            .binary_search_by(|compared_tid| {
                removed_will_fire_at.cmp(&timers.get(compared_tid.0).unwrap().will_fire_at)
            })
            .unwrap();
        self.index.remove(position);
    }

    fn remove_timer(&mut self, timer_id: TimerId) {
        let pos = timer_id.0;
        self.remove_from_index(timer_id);
        self.timers.remove(pos);
    }
}

pub(crate) struct Timer<'f, A, S> {
    will_fire_at: Duration,
    timer_state: TimerState<'f, A, S>,
}

enum TimerState<'f, A, S> {
    Cancelled,
    Calling,
    OneShot(Box<dyn FnOnce(&mut A, &mut S, &TimerId) -> TimerId + 'f>),
    Periodic(Duration, Box<dyn FnMut(&mut A, &mut S, &TimerId) + 'f>),
}

/// Возвращает системное время с микросекундным разрешением.
/// В качестве источника времени используются монотонные часы, отсчитывающие время с момента старта операционной системы (uptime)
#[cfg(not(test))]
pub fn get_system_time() -> Duration {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
    Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
}

#[cfg(test)]
pub fn get_system_time() -> Duration {
    test::TEST_SYS_TIME.with(|tst| *tst.borrow())
}

#[cfg(test)]
mod test {
    use crate::timers::{TimerId, Timers};
    use std::cell::RefCell;
    use std::ops::AddAssign;
    use std::time::Duration;

    thread_local!(pub(super) static TEST_SYS_TIME: RefCell<Duration> = RefCell::new(Duration::from_secs(0)));

    pub(super) fn advance_testing_system_time(advance: Duration) {
        TEST_SYS_TIME.with(|tst| tst.borrow_mut().add_assign(advance))
    }

    struct TestSys<'a> {
        timers: Timers<'a, TestApp, Self>,
    }

    impl<'a> TestSys<'a> {
        fn new() -> Self {
            TestSys {
                timers: Timers::new(),
            }
        }

        fn get_timers<'r>(&'r mut self) -> &'r mut Timers<'a, TestApp, Self> {
            &mut self.timers
        }
    }

    #[derive(Default)]
    struct TestApp {
        oneshot: Option<TimerId>,
        oneshot_counter: usize,
        periodic: Option<TimerId>,
        periodic_counter: usize,
    }

    #[test]
    fn test_oneshot() {
        let mut test_app: TestApp = Default::default();
        let mut test_sys = TestSys::new();
        test_app.oneshot = Some(test_sys.timers.set_oneshot(
            Duration::from_secs(1),
            |app, _, _| {
                app.oneshot_counter += 1;
                app.oneshot.take().unwrap()
            },
        ));
        advance_testing_system_time(test_sys.timers.delay_to_next_timer().unwrap());
        Timers::process_timers(&mut test_app, &mut test_sys, TestSys::get_timers);
        assert_eq!(test_app.oneshot_counter, 1);
    }

    #[test]
    fn test_periodic() {
        let mut test_app: TestApp = Default::default();
        let mut test_sys = TestSys::new();
        let _ = test_sys
            .timers
            .set_periodic(Duration::from_secs(1), |app, _, _| {
                app.periodic_counter += 1;
            });
        for i in 0..5 {
            advance_testing_system_time(test_sys.timers.delay_to_next_timer().unwrap());
            assert_eq!(test_app.periodic_counter, i);
            Timers::process_timers(&mut test_app, &mut test_sys, TestSys::get_timers);
            assert_eq!(test_app.oneshot_counter, 0);
            assert_eq!(test_app.periodic_counter, i + 1);
        }
    }

    #[test]
    fn test_periodic_with_cancel() {
        let mut test_app: TestApp = Default::default();
        let mut test_sys = TestSys::new();

        test_app.periodic = Some(test_sys.timers.set_periodic(
            Duration::from_secs(1),
            |app, sys, _| {
                app.periodic_counter += 1;
                if app.periodic_counter > 5 {
                    sys.timers.cancel(app.periodic.take().unwrap())
                }
            },
        ));

        for i in 0..5 {
            advance_testing_system_time(test_sys.timers.delay_to_next_timer().unwrap());
            assert_eq!(test_app.periodic_counter, i);
            Timers::process_timers(&mut test_app, &mut test_sys, TestSys::get_timers);
            assert_eq!(test_app.oneshot_counter, 0);
            assert_eq!(test_app.periodic_counter, i + 1);
        }
        advance_testing_system_time(test_sys.timers.delay_to_next_timer().unwrap());
        Timers::process_timers(&mut test_app, &mut test_sys, TestSys::get_timers);
        assert_eq!(test_app.periodic_counter, 6);

        advance_testing_system_time(Duration::from_millis(1000));
        Timers::process_timers(&mut test_app, &mut test_sys, TestSys::get_timers);
        assert_eq!(test_app.periodic_counter, 6);

        assert!(test_sys.timers.index.is_empty());
        assert!(test_sys.timers.timers.is_empty());
    }

    #[test]
    fn test_complex() {
        let mut test_app: TestApp = Default::default();
        let mut test_sys = TestSys::new();
        // Однократный таймер через 5 секунд
        test_app.oneshot = Some(test_sys.timers.set_oneshot(
            Duration::from_secs(5),
            |app, _, _| {
                app.oneshot_counter += 1;
                app.oneshot.take().unwrap()
            },
        ));
        assert!(test_app.oneshot.is_some());
        // Секунда 1
        advance_testing_system_time(Duration::from_millis(1000));
        Timers::process_timers(&mut test_app, &mut test_sys, TestSys::get_timers);
        assert_eq!(test_app.oneshot_counter, 0);
        // Секунда 2
        advance_testing_system_time(Duration::from_millis(1000));
        Timers::process_timers(&mut test_app, &mut test_sys, TestSys::get_timers);
        assert_eq!(test_app.oneshot_counter, 0);
        // Периодический таймер каждые 350 мс.
        let periodic_id = test_sys
            .timers
            .set_periodic(Duration::from_millis(350), |app, _, _| {
                app.periodic_counter += 1;
            });
        // Секунда 3
        advance_testing_system_time(Duration::from_millis(1000));
        Timers::process_timers(&mut test_app, &mut test_sys, TestSys::get_timers);
        assert_eq!(test_app.oneshot_counter, 0);
        assert_eq!(test_app.periodic_counter, 2);
        // Секунда 4
        advance_testing_system_time(Duration::from_millis(1000));
        Timers::process_timers(&mut test_app, &mut test_sys, TestSys::get_timers);
        assert_eq!(test_app.oneshot_counter, 0);
        assert_eq!(test_app.periodic_counter, 5);
        test_sys.timers.cancel(periodic_id);
        // Секунда 5
        advance_testing_system_time(Duration::from_millis(1000));
        Timers::process_timers(&mut test_app, &mut test_sys, TestSys::get_timers);
        assert!(test_app.oneshot.is_none());
        assert_eq!(test_app.oneshot_counter, 1);
        assert_eq!(test_app.periodic_counter, 5);
    }

    #[test]
    fn test_timers_order() {
        let mut test_sys = TestSys::new();

        let tid7 = test_sys
            .timers
            .set_periodic(Duration::from_secs(7), |_, _, _| {});

        let tid3 = test_sys
            .timers
            .set_periodic(Duration::from_secs(3), |_, _, _| {});

        let tid9 = test_sys
            .timers
            .set_periodic(Duration::from_secs(9), |_, _, _| {});

        let tid1 = test_sys
            .timers
            .set_periodic(Duration::from_secs(1), |_, _, _| {});

        let tid4 = test_sys
            .timers
            .set_periodic(Duration::from_secs(4), |_, _, _| {});

        let tid2 = test_sys
            .timers
            .set_periodic(Duration::from_secs(2), |_, _, _| {});

        let index = &test_sys.timers.index;
        assert_eq!(tid1, index[5]);
        assert_eq!(tid2, index[4]);
        assert_eq!(tid3, index[3]);
        assert_eq!(tid4, index[2]);
        assert_eq!(tid7, index[1]);
        assert_eq!(tid9, index[0]);

        test_sys.timers.cancel(tid4);
        let index = &test_sys.timers.index;
        assert_eq!(tid1, index[4]);
        assert_eq!(tid2, index[3]);
        assert_eq!(tid3, index[2]);
        assert_eq!(tid7, index[1]);
        assert_eq!(tid9, index[0]);

        test_sys.timers.cancel(tid1);
        let index = &test_sys.timers.index;
        assert_eq!(tid2, index[3]);
        assert_eq!(tid3, index[2]);
        assert_eq!(tid7, index[1]);
        assert_eq!(tid9, index[0]);

        test_sys.timers.cancel(tid9);
        let index = &test_sys.timers.index;
        assert_eq!(tid2, index[2]);
        assert_eq!(tid3, index[1]);
        assert_eq!(tid7, index[0]);
    }
}
