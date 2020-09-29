use crate::{AsSocketId, MaybeSocketOwner, ActorioContext, SocketHolder, SocketId};
use mio::event::Source;
use mio::unix::SourceFd;
use mio::{Interest, Registry, Token};
use std::io::{Error, ErrorKind};
use std::os::unix::io::RawFd;
use std::time::Duration;

#[derive(Hash, PartialEq, Eq, Debug)]
pub struct HPTimerId(SocketId);

impl AsSocketId for HPTimerId {
    fn as_socket_id(&self) -> &SocketId {
        &self.0
    }
}

impl From<SocketId> for HPTimerId {
    fn from(socket_id: SocketId) -> Self {
        HPTimerId(socket_id)
    }
}

pub(crate) struct HPTimer<'f, A> {
    timer_fd: TimerFd,
    hp_timer_state: HPTimerState<'f, A, ActorioContext<'f, A>>,
}

impl<'a, A> HPTimer<'a, A> {
    pub(crate) fn new(act_ctx: &mut ActorioContext<A>) -> Result<HPTimerId, Error> {
        let timer_fd = TimerFd::new()?;
        let hptimer = HPTimer {
            timer_fd,
            hp_timer_state: HPTimerState::Free,
        };
        act_ctx
            .register_socket_holder(SocketHolder::new(true, false, From::from(hptimer)))
            .map(From::from)
    }

    pub(crate) fn set_oneshot(
        &mut self,
        delay: Duration,
        func: impl FnOnce(&mut A, &mut ActorioContext<'a, A>, &HPTimerId) -> HPTimerId + 'a,
    ) -> Result<(), Error> {
        match self.hp_timer_state {
            HPTimerState::Free => {
                self.timer_fd.arm_oneshot(delay)?;
                self.hp_timer_state = HPTimerState::OneShot(Box::new(func));
            }
            _ => panic!("Unexpected state for HPTimer!"),
        }
        Ok(())
    }

    pub(crate) fn set_absolute(
        &mut self,
        deadline: Duration,
        func: impl FnOnce(&mut A, &mut ActorioContext<'a, A>, &HPTimerId) -> HPTimerId + 'a,
    ) -> Result<(), Error> {
        match self.hp_timer_state {
            HPTimerState::Free => {
                self.timer_fd.arm_absolute(deadline)?;
                self.hp_timer_state = HPTimerState::OneShot(Box::new(func));
            }
            _ => panic!("Unexpected state for HPTimer!"),
        }
        Ok(())
    }

    pub(crate) fn set_periodic(
        &mut self,
        interval: Duration,
        func: impl FnMut(&mut A, &mut ActorioContext<'a, A>, &HPTimerId) + 'a,
    ) -> Result<(), Error> {
        match self.hp_timer_state {
            HPTimerState::Free => {
                self.timer_fd.arm_periodic(interval)?;
                self.hp_timer_state = HPTimerState::Periodic(Box::new(func));
            }
            _ => panic!("Unexpected state for HPTimer!"),
        }
        Ok(())
    }

    pub(crate) fn cancel(&mut self) -> Result<(), Error> {
        match self.hp_timer_state {
            HPTimerState::OneShot(_) | HPTimerState::Periodic(_) | HPTimerState::Calling => {
                self.timer_fd.disarm()?;
                self.hp_timer_state = HPTimerState::Free;
            }
            HPTimerState::Free => panic!("Attempting to cancel free, not armed HPTimer!"),
        }
        Ok(())
    }

    pub(crate) fn process_hptimer(
        act_ctx: &mut ActorioContext<A>,
        app: &mut A,
        hptimer_id: HPTimerId,
    ) {
        if let Some(hptimer) = act_ctx.try_get_socket(&hptimer_id) {
            // Таймер обязательно надо вычитать, иначе не будет следующих срабатываний.
            let expired_count = hptimer.timer_fd.read();
            log::warn!(
                "{:?} expired more than one time: {}. Detected possible system overload!",
                hptimer_id,
                expired_count
            );

            let hptimer_state =
                std::mem::replace(&mut hptimer.hp_timer_state, HPTimerState::Calling);
            match hptimer_state {
                HPTimerState::OneShot(func) => {
                    let oneshot_id = func(app, act_ctx, &hptimer_id);
                    assert_eq!(
                        oneshot_id, hptimer_id,
                        "Returned oneshot HPTimerId is not same as called!"
                    );
                    act_ctx.close_socket_by_id(hptimer_id.as_socket_id());
                }
                HPTimerState::Periodic(mut func) => {
                    func(app, act_ctx, &hptimer_id);
                    if let Some(hptimer) = act_ctx.try_get_socket(&hptimer_id) {
                        match hptimer.hp_timer_state {
                            HPTimerState::Calling => {
                                hptimer.hp_timer_state = HPTimerState::Periodic(func)
                            }
                            _ => panic!("Unexpected state of HPTimer after invoking callback!"),
                        }
                    }
                }
                _ => panic!("Unexpected state of HPTimer before invoking callback!"),
            }
        }
    }
}

impl<'a, A> Source for HPTimer<'a, A> {
    fn register(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> Result<(), Error> {
        registry.register(&mut SourceFd(&self.timer_fd.raw_fd), token, interests)
    }

    fn reregister(
        &mut self,
        registry: &Registry,
        token: Token,
        interests: Interest,
    ) -> Result<(), Error> {
        registry.reregister(&mut SourceFd(&self.timer_fd.raw_fd), token, interests)
    }

    fn deregister(&mut self, registry: &Registry) -> Result<(), Error> {
        registry.deregister(&mut SourceFd(&self.timer_fd.raw_fd))
    }
}

enum HPTimerState<'f, A, S> {
    Free,
    Calling,
    OneShot(Box<dyn FnOnce(&mut A, &mut S, &HPTimerId) -> HPTimerId + 'f>),
    Periodic(Box<dyn FnMut(&mut A, &mut S, &HPTimerId) + 'f>),
}

struct TimerFd {
    raw_fd: RawFd,
}

impl Drop for TimerFd {
    fn drop(&mut self) {
        unsafe { libc::close(self.raw_fd) };
    }
}

impl TimerFd {
    fn new() -> Result<Self, Error> {
        unsafe {
            let result = libc::timerfd_create(libc::CLOCK_MONOTONIC, libc::TFD_NONBLOCK);
            if result == -1 {
                let err_code = *libc::__errno_location();
                Err(Error::from_raw_os_error(err_code))
            } else {
                Ok(TimerFd { raw_fd: result })
            }
        }
    }

    /// Таймер, после срабатывания события Readable в poll, должен быть прочитан однократно этой функцией.
    /// Это необходимо сделать, иначе следующие события в poll не поступят пока дескриптор не будет прочитан.
    /// Возвращает число срабатываний таймера, должно быть равно 1.
    /// Если более 1, вероятно приложение было перегружено событиями и не успело обработать таймер между срабатываниями, либо интервал слишком мал для системы.
    fn read(&mut self) -> u64 {
        let err = loop {
            match read_from_timer_fd(self.raw_fd) {
                Ok(counter) => return counter,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => break e,
            }
        };
        panic!("Error on reading from timer_fd({}): {}", self.raw_fd, err);
    }

    fn arm_periodic(&self, interval: Duration) -> Result<(), Error> {
        let it = new_periodic_its(interval);
        self.timerfd_settime(0, &it)
    }

    fn arm_absolute(&self, systime: Duration) -> Result<(), Error> {
        let it = new_oneshot_its(systime);
        self.timerfd_settime(libc::TFD_TIMER_ABSTIME, &it)
    }

    fn arm_oneshot(&self, delay: Duration) -> Result<(), Error> {
        let it = new_oneshot_its(delay);
        self.timerfd_settime(0, &it)
    }

    fn disarm(&self) -> Result<(), Error> {
        let it = new_zeroed_its();
        self.timerfd_settime(0, &it)
    }

    fn timerfd_settime(&self, flags: libc::c_int, it: &libc::itimerspec) -> Result<(), Error> {
        unsafe {
            let result = libc::timerfd_settime(self.raw_fd, flags, it, std::ptr::null_mut());
            if result == -1 {
                let err_code = *libc::__errno_location();
                Err(Error::from_raw_os_error(err_code))
            } else {
                Ok(())
            }
        }
    }
}

fn read_from_timer_fd(timer_fd: RawFd) -> Result<u64, Error> {
    let mut data = [0u8; 8];
    unsafe {
        let result = libc::read(timer_fd, data.as_mut_ptr() as *mut _, data.len());
        if result == -1 {
            let err_code = *libc::__errno_location();
            Err(Error::from_raw_os_error(err_code))
        } else {
            Ok(u64::from_ne_bytes(data))
        }
    }
}

fn new_periodic_its(interval: Duration) -> libc::itimerspec {
    libc::itimerspec {
        it_interval: libc::timespec {
            tv_sec: interval.as_secs() as libc::time_t,
            tv_nsec: interval.subsec_nanos() as libc::c_long,
        },
        it_value: libc::timespec {
            tv_sec: interval.as_secs() as libc::time_t,
            tv_nsec: interval.subsec_nanos() as libc::c_long,
        },
    }
}

fn new_oneshot_its(duration: Duration) -> libc::itimerspec {
    libc::itimerspec {
        it_interval: libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        },
        it_value: libc::timespec {
            tv_sec: duration.as_secs() as libc::time_t,
            tv_nsec: duration.subsec_nanos() as libc::c_long,
        },
    }
}

fn new_zeroed_its() -> libc::itimerspec {
    libc::itimerspec {
        it_interval: libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        },
        it_value: libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        },
    }
}

//RLTC-START-TESTS
#[cfg(test)]
mod tests {
    use crate::hp_timers::{
        new_oneshot_its, new_periodic_its, new_zeroed_its, read_from_timer_fd, HPTimerId, TimerFd,
    };
    use crate::{Application, ActorioContext};
    use std::io::{Error, ErrorKind};
    use std::time::Duration;

    /// В модуле обычных таймеров также есть такая функция, но она перекрывается заглушкой в тестах,
    /// поэтому вызывать её тут нельзя, точным таймерам нужны настоящие часы Linux.
    fn get_system_time() -> Duration {
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut ts) };
        Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
    }

    #[test]
    fn test_get_system_time() {
        let start_systime = get_system_time();
        std::thread::sleep(Duration::from_millis(100));
        let stop_systime = get_system_time();
        let diff = stop_systime - start_systime;
        let diff = diff.as_millis();
        assert!(diff - 100 < 1); // На загруженной машине тест может провалится
    }

    #[test]
    fn test_timer_nonblock() {
        let timer = TimerFd::new().unwrap();
        assert_eq!(
            read_from_timer_fd(timer.raw_fd).unwrap_err().kind(),
            ErrorKind::WouldBlock
        );
    }

    #[test]
    fn test_new_zeroed_its() {
        let its = new_zeroed_its();
        assert_eq!(its.it_interval.tv_sec, 0);
        assert_eq!(its.it_interval.tv_nsec, 0);
        assert_eq!(its.it_value.tv_sec, 0);
        assert_eq!(its.it_value.tv_nsec, 0);
    }

    #[test]
    fn test_new_oneshot_its() {
        let duration = Duration::from_millis(1000);
        let its = new_oneshot_its(duration);
        assert_eq!(its.it_interval.tv_sec, 0);
        assert_eq!(its.it_interval.tv_nsec, 0);
        assert_eq!(its.it_value.tv_sec, duration.as_secs() as libc::time_t);
        assert_eq!(
            its.it_value.tv_nsec,
            duration.subsec_nanos() as libc::c_long
        );
    }

    #[test]
    fn test_new_periodic_its() {
        let duration = Duration::from_millis(1000);
        let its = new_periodic_its(duration);
        assert_eq!(its.it_interval.tv_sec, duration.as_secs() as libc::time_t);
        assert_eq!(
            its.it_interval.tv_nsec,
            duration.subsec_nanos() as libc::c_long
        );
        assert_eq!(its.it_value.tv_sec, duration.as_secs() as libc::time_t);
        assert_eq!(
            its.it_value.tv_nsec,
            duration.subsec_nanos() as libc::c_long
        );
    }

    /// Этот тест может проваливаться, если машина перегружена и не выдерживаются интервалы sleep как в тесте (хотя и взяты с запасом)
    #[test]
    fn test_timerfd_oneshot() {
        let timer = TimerFd::new().unwrap();
        timer.arm_oneshot(Duration::from_millis(30)).unwrap();
        std::thread::sleep(Duration::from_millis(2));
        // Пока не сработал
        assert_eq!(
            read_from_timer_fd(timer.raw_fd).unwrap_err().kind(),
            ErrorKind::WouldBlock
        );
        std::thread::sleep(Duration::from_millis(40));
        // Стработал один раз
        assert_eq!(read_from_timer_fd(timer.raw_fd).unwrap(), 1);
        std::thread::sleep(Duration::from_millis(40));
        // Не срабатывает больше
        assert_eq!(
            read_from_timer_fd(timer.raw_fd).unwrap_err().kind(),
            ErrorKind::WouldBlock
        );
    }

    /// Этот тест может проваливаться, если машина перегружена и не выдерживаются интервалы sleep как в тесте (хотя и взяты с запасом)
    #[test]
    fn test_timerfd_absolute() {
        let timer = TimerFd::new().unwrap();
        timer
            .arm_absolute(get_system_time() + Duration::from_millis(30))
            .unwrap();
        std::thread::sleep(Duration::from_millis(2));
        // Пока не сработал
        assert_eq!(
            read_from_timer_fd(timer.raw_fd).unwrap_err().kind(),
            ErrorKind::WouldBlock
        );
        std::thread::sleep(Duration::from_millis(40));
        // Сработал один раз
        assert_eq!(read_from_timer_fd(timer.raw_fd).unwrap(), 1);
        std::thread::sleep(Duration::from_millis(40));
        // Не срабатывает больше
        assert_eq!(
            read_from_timer_fd(timer.raw_fd).unwrap_err().kind(),
            ErrorKind::WouldBlock
        );
        // Таймер в прошлом
        timer.arm_absolute(get_system_time()).unwrap();
        std::thread::sleep(Duration::from_millis(1));
        // Сработал один раз
        assert_eq!(read_from_timer_fd(timer.raw_fd).unwrap(), 1);
    }

    /// Этот тест может проваливаться, если машина перегружена и не выдерживаются интервалы sleep как в тесте (хотя и взяты с запасом)
    #[test]
    fn test_timerfd_periodic() {
        let timer = TimerFd::new().unwrap();
        timer.arm_periodic(Duration::from_millis(30)).unwrap();
        std::thread::sleep(Duration::from_millis(2));
        // Пока не сработал
        assert_eq!(
            read_from_timer_fd(timer.raw_fd).unwrap_err().kind(),
            ErrorKind::WouldBlock
        );
        std::thread::sleep(Duration::from_millis(40));
        // Сработал один раз
        assert_eq!(read_from_timer_fd(timer.raw_fd).unwrap(), 1);
        std::thread::sleep(Duration::from_millis(40));
        // Сработал еще раз
        assert_eq!(read_from_timer_fd(timer.raw_fd).unwrap(), 1);
    }

    /// Этот тест может проваливаться, если машина перегружена и не выдерживаются интервалы sleep как в тесте (хотя и взяты с запасом)
    #[test]
    fn test_timerfd_disarm() {
        let timer = TimerFd::new().unwrap();
        timer.arm_periodic(Duration::from_millis(30)).unwrap();
        std::thread::sleep(Duration::from_millis(2));
        // Пока не сработал
        assert_eq!(
            read_from_timer_fd(timer.raw_fd).unwrap_err().kind(),
            ErrorKind::WouldBlock
        );
        std::thread::sleep(Duration::from_millis(40));
        // Сработал один раз
        assert_eq!(read_from_timer_fd(timer.raw_fd).unwrap(), 1);
        std::thread::sleep(Duration::from_millis(40));
        // Сработал еще раз
        assert_eq!(read_from_timer_fd(timer.raw_fd).unwrap(), 1);
        // Отключили
        timer.disarm().unwrap();
        std::thread::sleep(Duration::from_millis(40));
        // Больше не срабатывает
        assert_eq!(
            read_from_timer_fd(timer.raw_fd).unwrap_err().kind(),
            ErrorKind::WouldBlock
        );
    }

    type OnStart = Box<
        dyn for<'a> FnOnce(&mut TestApp, &mut ActorioContext<'a, TestApp>) -> Result<(), Error>
            + 'static,
    >;

    struct TestApp {
        oneshot: Option<HPTimerId>,
        oneshot_counter: usize,
        periodic: Option<HPTimerId>,
        periodic_counter: usize,
        on_start: Option<OnStart>,
    }

    impl TestApp {
        fn new(
            func: impl FnOnce(&mut TestApp, &mut ActorioContext<'_, TestApp>) -> Result<(), Error>
                + 'static,
        ) -> Self {
            TestApp {
                oneshot: None,
                oneshot_counter: 0,
                periodic: None,
                periodic_counter: 0,
                on_start: Some(Box::new(func)),
            }
        }
    }

    impl Application for TestApp {
        fn start<'app>(&mut self, act_ctx: &mut ActorioContext<'app, Self>) -> Result<(), Error> {
            (self.on_start.take().unwrap())(self, act_ctx)
        }
    }

    #[test]
    fn test_hptimer_oneshot() {
        let test_app = TestApp::new(|app, act_ctx| {
            app.oneshot = Some(act_ctx.hptimer_set_oneshot(
                Duration::from_millis(10),
                |app, sys, _| {
                    app.oneshot_counter += 1;
                    sys.stop();
                    app.oneshot.take().unwrap()
                },
            ));
            Ok(())
        });
        let test_app = ActorioContext::run(test_app).unwrap();
        assert_eq!(test_app.oneshot_counter, 1);
    }

    #[test]
    fn test_hptimer_absolute() {
        let test_app = TestApp::new(|app, act_ctx| {
            app.oneshot = Some(act_ctx.hptimer_set_absolute(
                get_system_time() + Duration::from_millis(10),
                |app, sys, _| {
                    app.oneshot_counter += 1;
                    sys.stop();
                    app.oneshot.take().unwrap()
                },
            ));
            Ok(())
        });
        let test_app = ActorioContext::run(test_app).unwrap();
        assert_eq!(test_app.oneshot_counter, 1);
    }

    #[test]
    fn test_hptimer_periodic() {
        let test_app = TestApp::new(|app, act_ctx| {
            app.periodic = Some(act_ctx.hptimer_set_periodic(
                Duration::from_millis(10),
                |app, sys, _| {
                    app.periodic_counter += 1;
                    if app.periodic_counter == 5 {
                        sys.hptimer_cancel(app.periodic.take().unwrap());
                        sys.stop();
                    }
                },
            ));
            Ok(())
        });
        let test_app = ActorioContext::run(test_app).unwrap();
        assert_eq!(test_app.periodic_counter, 5);
    }
}
//RLTC-END-TESTS
