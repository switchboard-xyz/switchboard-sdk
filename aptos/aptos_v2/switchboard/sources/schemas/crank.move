module switchboard::crank {
    use aptos_framework::timestamp;
    use std::error;
    use std::vector;
    use switchboard::errors;

    friend switchboard::crank_init_action;
    friend switchboard::crank_push_action;
    friend switchboard::crank_pop_action;

    struct CrankRow has store, drop, copy {
        aggregator_addr: address,
        timestamp: u64,
    }

    struct Crank has key, store, drop {
        heap: vector<CrankRow>, // Binary Heap is a priority queue
        queue_addr: address, // Crank is bound to a queue
        created_at: u64,
        jitter_modifier: u64,
        features: vector<bool>,
        _ebuf: vector<u8>,
    }

    public fun peak(self: address, pop_idx: u64): (address, u64) acquires Crank {
        let crank = borrow_global<Crank>(self);
        let row = vector::borrow<CrankRow>(&crank.heap, pop_idx);
        (row.aggregator_addr, row.timestamp)
    }

    public fun jitter_modifier(self: address): u64 acquires Crank {
        let crank = borrow_global<Crank>(self);
        crank.jitter_modifier
    }

    public fun exist(addr: address): bool {
        exists<Crank>(addr)
    }

    fun parent(index: u64): u64 {
        if (index == 0) {
            0
        }  else {
            (index - 1) / 2
        }
    }

    fun left_child(num: u64): u64 {
        (num * 2) + 1
    }

    fun right_child(num: u64): u64 {
        (num * 2) + 2
    }

    public(friend) fun new(queue_addr: address): Crank {
        Crank {
            heap: vector::empty(),
            queue_addr,
            created_at: timestamp::now_seconds(),
            jitter_modifier: 0,
            features: vector::empty(),
            _ebuf: vector::empty(),
        }
    }

    public(friend) fun crank_create(account: &signer, crank: Crank) {
        move_to(account, crank);
    }

    public(friend) fun size(self: address): u64 acquires Crank {
        vector::length(&borrow_global<Crank>(self).heap)
    }

    // Anybody can push a feed to a crank
    public(friend) fun push(self: address, aggregator_addr: address, timestamp: u64) acquires Crank {
        let crank = borrow_global_mut<Crank>(self);
        let heap_size = vector::length(&crank.heap);

        // build new CrankRow and add it to the back of heap vector
        vector::push_back(&mut crank.heap, CrankRow {
            aggregator_addr,

            // make this the next timestamp for aggregator update
            timestamp,
        });

        let current: u64 = heap_size;

        // enforce that parent timestamp is greater than current
        while (current != 0 
            && vector::borrow<CrankRow>(&crank.heap, current).timestamp < vector::borrow<CrankRow>(&crank.heap, parent(current)).timestamp)
        {
            let parent = parent(current);
            vector::swap<CrankRow>(&mut crank.heap, current, parent);
            current = parent;
        }
    }

    public(friend) fun increment_jitter_modifier(self: address) acquires Crank {
        let crank = borrow_global_mut<Crank>(self);
        crank.jitter_modifier = (crank.jitter_modifier + 1) % 5; 
    }


    /**
     * pop the crank, but also updates and returns the jitter modifier 
     */
    public(friend) fun pop(self: address, pop_idx: u64): (
        address, // aggregator address
        u64,     // update timestamp
        u64,     // jitter modifier
    ) acquires Crank {
        let crank = borrow_global_mut<Crank>(self);
        let size = vector::length(&crank.heap);

        // enforce constraint that 
        assert!(size != 0, error::internal(errors::CrankEmpty()));

        // swap peak and ideally lowest element 
        vector::swap<CrankRow>(&mut crank.heap, pop_idx, size - 1);

        // pop the peak
        let popped = vector::pop_back(&mut crank.heap);
        size = size - 1;

        // re-heapify (enforce the constraint that children timestamps are greater than parent)
        let current = pop_idx;
        loop {
            let index = right_child(current);
            let left = left_child(current);

            // if right child index is greater than size of heap, swap index for left
            if (index >= size) {
                index = left;

            // if right is greater than left, swap the value of index (formerly holding right index) for that of left
            } else if (vector::borrow<CrankRow>(&crank.heap, index).timestamp > vector::borrow<CrankRow>(&crank.heap, left).timestamp) {
                index = left;
            };

            // if the left child of current is greater than the size of the heap, stop
            if (index >= size) {
                break
            };

            if (vector::borrow<CrankRow>(&crank.heap, current).timestamp < vector::borrow<CrankRow>(&crank.heap, index).timestamp) {
                break
            };
            
            vector::swap(&mut crank.heap, current, index);
            current = index;
        };

        let jitter_modifier = crank.jitter_modifier;
        crank.jitter_modifier = (crank.jitter_modifier + 1) % 5; 

        // return aggregator address (the one with the lowest timestamp)
        (popped.aggregator_addr, popped.timestamp, jitter_modifier)
    }
}
