use std::{cell::UnsafeCell, hint::spin_loop, mem::MaybeUninit, sync::atomic::{AtomicI64, AtomicU64, Ordering, fence}};

#[derive(Debug)]
pub struct JobBlock {
    used_slots: AtomicU64,
    jobs: [JobData; Self::LEN]
}

impl JobBlock {
    /// The number of jobs that a single block can hold.
    pub const LEN: usize = u64::BITS as usize;

    pub fn help(&self, masks: JobBlockMasks) -> bool {
        let mut ran_job = false;
        while self.help_one(masks) {
            ran_job = true;
        }
        ran_job
    }

    pub fn help_one(&self, masks: JobBlockMasks) -> bool {
        let mut to_check = masks.load.0.load(Ordering::Acquire);
        loop {
            let slot = to_check.trailing_zeros() as usize;
            
            if slot == Self::LEN {
                return false;
            }

            to_check &= u64::MAX << slot;

            let job = &self.jobs[slot];
            let reserved_unit = job.available_units.fetch_sub(1, Ordering::Relaxed) - 1;

            if reserved_unit < 0 {
                continue;
            }
            else if masks.load.get(slot) {
                // Safety: we were able to reserve a work unit, so the job is valid
                // and will remain so until the unit is processed.
                unsafe {
                    (*job.func.get().read().assume_init())(JobInvocation {
                        clear_masks: masks.store,
                        job,
                        reserved_unit,
                        slot
                    });
                };

                return true;
            }
            else {
                // The job is not in the mask any more
                job.available_units.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn help_while(&self, masks: JobBlockMasks, mut f: impl FnMut() -> bool) -> bool {
        let mut ran_job = false;
        
        while self.help_one(masks) {
            ran_job = true;

            if !f() {
                break;
            }
        }

        ran_job
    }

    pub fn try_execute(&self, f: impl Fn(usize) + Sync, times: usize, masks: JobBlockMasks) -> bool {
        match times {
            0 => true,
            1 => { f(0); true },
            _ => if let Some(slot) = self.reserve_slot() {
                let job = &self.jobs[slot];

                let func = Self::create_job_func(f);

                // Safety: this slot was reserved; there should not be any other threads referencing it
                unsafe { job.func.get().write(MaybeUninit::new(Self::job_func_ptr(&func))); }
                job.incomplete_units.store(times as i64, Ordering::Relaxed);
                job.available_units.store(times as i64 - 1, Ordering::Relaxed);

                for mask in masks.store {
                    mask.set(slot, true);
                }

                // Safety: first work unit has been reserved
                unsafe {
                    (*job.func.get().read().assume_init())(JobInvocation {
                        clear_masks: masks.store,
                        job,
                        reserved_unit: times as i64 - 1,
                        slot
                    });
                };

                while 0 < job.incomplete_units.load(Ordering::Relaxed) {
                    if !self.help(masks) {
                        // todo: use event
                        spin_loop();
                    }                    
                }

                fence(Ordering::SeqCst);

                self.used_slots.fetch_and(!(1 << slot), Ordering::Relaxed);

                true
            }
            else {
                false
            }
        }        
    }

    fn reserve_slot(&self) -> Option<usize> {
        loop {
            let slot = self.used_slots.load(Ordering::Relaxed).trailing_ones() as usize;
            if slot < Self::LEN {
                let mask = 1 << slot;
                let previous = self.used_slots.fetch_or(mask, Ordering::Relaxed);

                if (previous & mask) == 0 {
                    return Some(slot);
                }
            }
            else {
                return None;
            }
        }
    }

    fn create_job_func(f: impl Fn(usize) + Sync) -> impl Fn(JobInvocation) {
        move |invocation| {
            if invocation.reserved_unit == 0 {
                for mask in invocation.clear_masks {
                    mask.set(invocation.slot, false);
                }
            }

            let mut locally_completed = 1;
            f(invocation.reserved_unit as usize);

            loop {
                let next_unit = invocation.job.available_units.fetch_sub(1, Ordering::Relaxed) - 1;
                if next_unit < 0 {
                    break;
                }
                else if next_unit == 0 {
                    for mask in invocation.clear_masks {
                        mask.set(invocation.slot, false);
                    }
                }

                f(next_unit as usize);
                locally_completed += 1;
            }

            // Note release ordering
            let remaining_incomplete = invocation.job.incomplete_units.fetch_sub(locally_completed, Ordering::Release) - locally_completed;
            if remaining_incomplete == 0 {
                // notify event listener?
            }
        }
    }

    fn job_func_ptr(f: &impl Fn(JobInvocation)) -> *const dyn Fn(JobInvocation) {
        // Safety: it is always sound to create a pointer
        unsafe {
            std::mem::transmute::<
                *const (dyn Fn(JobInvocation) + '_),
                *const (dyn Fn(JobInvocation) + 'static),
            >(f as *const dyn Fn(JobInvocation))
        }
    }
}

impl Default for JobBlock {
    fn default() -> Self {
        Self {
            used_slots: AtomicU64::new(0),
            jobs: std::array::from_fn(|_| JobData::default())
        }
    }
}

#[derive(Debug, Default)]
pub struct JobBlockMask(AtomicU64);

impl JobBlockMask {
    pub fn get(&self, index: usize) -> bool {
        let mask = 1 << index;
        (self.0.load(Ordering::Acquire) & mask) != 0
    }

    pub fn set(&self, index: usize, value: bool) {
        let mask = 1 << index;
        if value {
            self.0.fetch_or(mask, Ordering::Release);
        }
        else {
            self.0.fetch_and(!mask, Ordering::Release);
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct JobBlockMasks<'a> {
    pub load: &'a JobBlockMask,
    pub store: &'a [&'a JobBlockMask],
}

#[derive(Debug)]
struct JobData {
    available_units: AtomicI64,
    incomplete_units: AtomicI64,
    func: UnsafeCell<MaybeUninit<*const dyn Fn(JobInvocation)>>
}

impl Default for JobData {
    fn default() -> Self {
        Self {
            available_units: AtomicI64::new(0),
            incomplete_units: AtomicI64::new(0),
            func: UnsafeCell::new(MaybeUninit::uninit())
        }
    }
}

#[derive(Debug)]
struct JobInvocation<'a> {
    clear_masks: &'a [&'a JobBlockMask],
    job: &'a JobData,
    reserved_unit: i64,
    slot: usize
}