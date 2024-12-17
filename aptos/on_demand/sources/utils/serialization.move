module switchboard::serialization {
    use switchboard::decimal::{Self, Decimal};
    use std::vector;
    use std::from_bcs;

    struct Bytes has store, copy, drop {
        bytes: vector<u8>
    }

    public fun new(bytes: vector<u8>): Bytes {
        vector::reverse(&mut bytes);
        Bytes { bytes }
    }

    public fun peel_vec(self: &mut Bytes, length: u64): vector<u8> {
        let (vec, i) = (vector::empty(), 0);
        while (i < length) {
            vector::push_back(&mut vec, vector::pop_back(&mut self.bytes));
            i = i + 1;
        };
        vec
    }

    public fun peel_address(self: &mut Bytes): address {
        let (addr_bytes, i) = (vector::empty(), 0);
        while (i < 32) {
            vector::push_back(&mut addr_bytes, vector::pop_back(&mut self.bytes));
            i = i + 1;
        };
        from_bcs::to_address(addr_bytes)
    }

    public fun peel_bool(self: &mut Bytes): bool {
        if (vector::pop_back(&mut self.bytes) == 1) {
            true
        } else {
            false
        }
    }

    public fun peel_u8(self: &mut Bytes): u8 {
        vector::pop_back(&mut self.bytes)
    }

    public fun peel_u16(self: &mut Bytes): u64 {
        let value = 0u64;        
        for (i in 0..2) {
            let byte = (vector::pop_back(&mut self.bytes) as u64);
            value = value + (byte << (8 * (1 - i)));
        };
        value
    }

    public fun peel_u64(self: &mut Bytes): u64 {
        let value = 0u64;
        for (i in 0..8) {
            let byte = (vector::pop_back(&mut self.bytes) as u64);
            value = value + (byte << (8 * (7 - i)));
        };
        value
    }

    public fun peel_u128(self: &mut Bytes): u128 {
        let value = 0u128;
        for (i in 0..16) {
            let byte = (vector::pop_back(&mut self.bytes) as u128);
            value = value + (byte << (8 * (15 - i)));
        };
        value
    }

    /*
      0-1 u8 discriminator
      Message type 1: Update
      1-33 bytes32 aggregator address
      33-49 int128 result
      49-81 bytes32 r
      81-113 bytes32 s
      113-114 uint8 v
      114-122 uint64 block number
      122-130 uint64 timestamp
      130-162 bytes32 oracle address
    */
    public fun parse_update_bytes(vec: vector<u8>): (
        // discriminator
        u8,
        // aggregator id
        address,
        // result
        Decimal,
        // r
        vector<u8>,
        // s
        vector<u8>,
        // v
        u8,
        // block number
        u64,
        // timestamp
        u64,
        // oracle key
        vector<u8>,
    ) {
        let bytes = new(vec);
        let discriminator = peel_u8(&mut bytes);
        assert!(discriminator == 1, 1337);
        let aggregator = peel_address(&mut bytes);
        let result = peel_u128(&mut bytes);
        
        // Define the 2^127 and 2^128 constants
        let two_pow_127: u128 = 170141183460469231731687303715884105728u128; // 2^127
        let two_pow_128: u256 = 340282366920938463463374607431768211456; // 2^128

        // Determine if the value is in the negative range
        let neg = result >= two_pow_127;

        // If neg, adjust to get the absolute value
        let abs_value = if (neg) {
            (two_pow_128 - (result as u256)) as u128
        } else {
            result
        };

        let value = decimal::new(abs_value, neg);
        let r = peel_vec(&mut bytes, 32);
        let s = peel_vec(&mut bytes, 32);
        let v = peel_u8(&mut bytes);
        let block_number = peel_u64(&mut bytes);
        let timestamp = peel_u64(&mut bytes);
        let oracle_key = peel_vec(&mut bytes, 32);

        /*  
            0x01
            6efd87f0f123a0b42de249592aabf61ca7abe2bf705fe6902cb51d093355e39f
            12605e09f5b493434000b5ee7ba92b6e
            01ac40f46148b08cefc7ee917ba0506b97ed84f84148d241f0ac178c656d69a4
            4596c47ac945799c5edbdaeeb403cd10f1ce4e56eb3fc90f71cc0000000000000000000000000000000000000000000000000000000000000000006734468321f9a5d199bf7665e18191fd191bca233def6fd8e939050d148eba0a815fc477
            0x
            01
            6efd87f0f123a0b42de249592aabf61ca7abe2bf705fe6902cb51d093355e39f
            000000000000125dd5afba1c39fe9c00
            0bac6888618f57a0347a0564a302734906bad6da789b36b08ee60ce9cf340996
            3d1ba37571b692e2ea5fcbbc7621520be932ed48dac95880c67a4bb1b580daad
            01
            0000000000000000000000000000000000000000000000000000000000000000
            67344d850000000050b706694cd4fa0da61c6ca9f77c44496085be4947877fdeb34df78d3342ac9d

            0x
            01
            6efd87f0f123a0b42de249592aabf61ca7abe2bf705fe6902cb51d093355e39f
            0000000000001260338e97dfd714b800
            6c77c09db7dc7df35a6991c9b6c42f2d1eb3449e21a39edd4ec887bcd7e3685d
            572e5b56e4bca34d4d991eea812cee545c0f68e7d3e20b7f55819da7ef407200
            0000000000000000
            0067344f3400000000015e3dd8f65e321814455da09eb4cc13e87a7332971afa4611fcc621fa3f0450
        */

        
        (discriminator, aggregator, value, r, s, v, block_number, timestamp, oracle_key)
    }

    /*
        0-1 u8 discriminator
        Message type 2: Attestation
        1-33 bytes32 oracle address
        33-65 bytes32 queue address
        65-97 bytes32 mr enclave
        97-161 bytes secp256k1 enclave key
        161-169 uint64 block number
        169-201 bytes32 r
        201-233 bytes32 s
        233-234 uint8 v
        234-242 uint64 timestamp
        242-274 bytes32 guardian address
    */
    public fun parse_attestation_bytes(vec: vector<u8>): (
        // discriminator
        u8,
        // oracle address
        address,
        // queue address
        address,
        // mr enclave
        vector<u8>,
        // secp256k1 key
        vector<u8>,
        // block number
        u64,
        // r
        vector<u8>,
        // s
        vector<u8>,
        // v
        u8,
        // timestamp
        u64,
        // guardian address
        address
    ) {
        let bytes = new(vec);
        let discriminator = peel_u8(&mut bytes);
        assert!(discriminator == 2, 1338);
        let oracle_address = peel_address(&mut bytes);
        let queue_address = peel_address(&mut bytes);
        let mr_enclave = peel_vec(&mut bytes, 32);
        let secp256k1_key = peel_vec(&mut bytes, 64);
        let block_number = peel_u64(&mut bytes);
        let r = peel_vec(&mut bytes, 32);
        let s = peel_vec(&mut bytes, 32);
        let v = peel_u8(&mut bytes);
        let timestamp = peel_u64(&mut bytes);
        let guardian_address = peel_address(&mut bytes);
        (discriminator, oracle_address, queue_address, mr_enclave, secp256k1_key, block_number, r, s, v, timestamp, guardian_address)
    }

    public fun get_message_discriminator(vec: &vector<u8>): u8 {
        if (vector::length(vec) < 1) {
            return 0
        };
        if (vector::length(vec) == 162 && *vector::borrow(vec, 0) == 1) {
            return 1
        } else if (vector::length(vec) == 274 && *vector::borrow(vec, 0) == 2) {
            return 2
        };
        0
    }

    #[test]
    fun test_parse_update_bytes() {
      
        let err: u64 = 1337;
        let vec = x"01be2fc90f8403af4dc48ec43aaf0e22b59d6fad16df9a8a4653d9aac122f47da2000000000000009d466073bb0b090000aa6baabfa6fbf760903892d2ff57f88d938711f85408278c3c2d5af8e2fbeeb70e5436639c1655bbdc0f08c2e254430445ab81a70131c277c86f6a46c3d904ea00000000000000000000000000672d332baaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let (discriminator, aggregator, value, r, s, v, block_number, timestamp, oracle_key) = parse_update_bytes(vec);
        assert!(discriminator == 1, err);
        assert!(aggregator == @0xbe2fc90f8403af4dc48ec43aaf0e22b59d6fad16df9a8a4653d9aac122f47da2, err);
        assert!(value == decimal::new(2901210000000000000000, false), err);
        assert!(r == x"aa6baabfa6fbf760903892d2ff57f88d938711f85408278c3c2d5af8e2fbeeb7", err);
        assert!(s == x"0e5436639c1655bbdc0f08c2e254430445ab81a70131c277c86f6a46c3d904ea", err);
        assert!(v == 0, err);
        assert!(block_number == 0, err);
        assert!(timestamp == 1731015467, err);
        assert!(oracle_key == x"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", err);
    }

    #[test]
    fun test_parse_attestation_bytes() {
        let err: u64 = 1338;
        let vec = x"0268e249436e79e4dfb69973faeff3f1543685121bb157c6da291cf0ec5ad4e553c9477bfb5ff1012859f336cf98725680e7705ba2abece17188cfb28ca66ca5b08be97da58eff5ecc4774ff779d397f0ff3712ff8e3c63001a01010cbaa8f7e36fb282eddb75bad021e1a490519b84e92492e8a8905171b92f9ab1929fc7df9c63a0910bbafec90f5231bc778a186936b85c0426085b27806008d8357238fce7b000000000000000024fa6d05f22fef8574a3e6dfc3a93a97e2a4a856bf14399e384e1a68ed92cefc1c3ebb34b489eb26d6ccb2dd771143b4810641bb68749e8710bc63786e46b0500000000000672d3b63347246323cd84b092828a32d197884b6296851a5a108d1d5b327a67344c72e33";
        let (discriminator, oracle_key, queue_key, mr_enclave, secp256k1_key, block_number, r, s, v, timestamp, guardian_key) = parse_attestation_bytes(vec);
        assert!(discriminator == 2, err);
        assert!(oracle_key == @0x68e249436e79e4dfb69973faeff3f1543685121bb157c6da291cf0ec5ad4e553, err);
        assert!(queue_key == @0xc9477bfb5ff1012859f336cf98725680e7705ba2abece17188cfb28ca66ca5b0, err);
        assert!(mr_enclave == x"8be97da58eff5ecc4774ff779d397f0ff3712ff8e3c63001a01010cbaa8f7e36", err);
        assert!(secp256k1_key == x"fb282eddb75bad021e1a490519b84e92492e8a8905171b92f9ab1929fc7df9c63a0910bbafec90f5231bc778a186936b85c0426085b27806008d8357238fce7b", err);
        assert!(block_number == 0, err);
        assert!(r == x"24fa6d05f22fef8574a3e6dfc3a93a97e2a4a856bf14399e384e1a68ed92cefc", err);
        assert!(s == x"1c3ebb34b489eb26d6ccb2dd771143b4810641bb68749e8710bc63786e46b050", err);
        assert!(v == 0, err);
        assert!(guardian_key == @0x347246323cd84b092828a32d197884b6296851a5a108d1d5b327a67344c72e33, err);
        assert!(timestamp == 1731017571, err);
    }

}