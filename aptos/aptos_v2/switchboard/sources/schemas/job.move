module switchboard::job {
    use aptos_framework::timestamp;
    use std::hash;
    use std::vector;

    friend switchboard::aggregator;
    friend switchboard::aggregator_add_job_action;
    friend switchboard::aggregator_remove_job_action;
    friend switchboard::job_init_action;

    struct Job has key, copy, drop {
        addr: address,
        name: vector<u8>,
        metadata: vector<u8>,
        authority: address,
        expiration: u64,
        hash: vector<u8>,
        data: vector<u8>,
        reference_count: u64,
        total_spent: u64,
        created_at: u64,
        variables: vector<vector<u8>>,
        features: vector<bool>,
        _ebuf: vector<u8>,
    }

    public fun addr(job: &Job): address {
        job.addr
    }

    public fun exist(addr: address): bool {
        exists<Job>(addr)
    }

    public(friend) fun job_create(account: &signer, job: Job) {
        move_to(account, job);
    }

    public(friend) fun job_get(addr: address): Job acquires Job {
        *borrow_global<Job>(addr)
    }

    public(friend) fun hash(addr: address): vector<u8> acquires Job {
        let job = borrow_global<Job>(addr);
        job.hash
    }

    public(friend) fun new(
        addr: address,
        name: vector<u8>,
        metadata: vector<u8>,
        authority: address,
        data: vector<u8>
    ): Job {
        Job {
            addr,
            name,
            metadata,
            authority,
            expiration: 0,
            hash: hash::sha3_256(data),
            reference_count: 0,
            total_spent: 0,
            created_at: timestamp::now_seconds(),
            variables: vector::empty(),
            data,
            features: vector::empty(),
            _ebuf: vector::empty(),
        }
    }

    public(friend) fun add_ref_count(addr: address) acquires Job {
        let job = borrow_global_mut<Job>(addr);
        job.reference_count = job.reference_count + 1;
    }

    public(friend) fun sub_ref_count(addr: address) acquires Job {
        let job = borrow_global_mut<Job>(addr);
        job.reference_count = job.reference_count - 1;
    }

}
