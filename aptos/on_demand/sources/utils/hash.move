module switchboard::hash {
    use std::hash;
    use std::bcs;
    use std::vector;
    use switchboard::decimal::{Self, Decimal};
    
    const MAX_U128: u128 = 340282366920938463463374607431768211455;

    struct Hasher has drop, copy {
        buffer: vector<u8>,
    }

    public fun new(): Hasher {
        Hasher {
            buffer: vector::empty(),
        }
    }

    public fun finalize(self: &Hasher): vector<u8> {
        hash::sha2_256(self.buffer)
    }

    public fun push_u8(self: &mut Hasher, value: u8) {
        vector::push_back(&mut self.buffer, value);
    }

    public fun push_u32(self: &mut Hasher, value: u32) {
        let bytes = bcs::to_bytes(&value);
        vector::reverse(&mut bytes);
        vector::append(&mut self.buffer, bytes);  
    }

    public fun push_u32_le(self: &mut Hasher, value: u32) {
        let bytes = bcs::to_bytes(&value);
        vector::append(&mut self.buffer, bytes);
    }

    public fun push_u64(self: &mut Hasher, value: u64) {
        let bytes = bcs::to_bytes(&value);
        vector::reverse(&mut bytes);
        vector::append(&mut self.buffer, bytes);
    }

    public fun push_u64_le(self: &mut Hasher, value: u64) {
        let bytes = bcs::to_bytes(&value);
        vector::append(&mut self.buffer, bytes);
    }

    public fun push_u128(self: &mut Hasher, value: u128) {
        let bytes = bcs::to_bytes(&value);
        vector::reverse(&mut bytes);
        vector::append(&mut self.buffer, bytes);
    }

    public fun push_i128(self: &mut Hasher, value: u128, neg: bool) {
        let signed_value: u128 = if (neg) {

            // Get two's complement by subtracting from 2^128
            MAX_U128 - value + 1
        } else {
            value
        };
        let bytes = bcs::to_bytes(&signed_value);
        vector::reverse(&mut bytes);
        vector::append(&mut self.buffer, bytes);
    }

    public fun push_i128_le(self: &mut Hasher, value: u128, neg: bool) {
        let signed_value: u128 = if (neg) {
            // Get two's complement by subtracting from 2^128
            MAX_U128 - value + 1
        } else {
            value
        };
        let bytes = bcs::to_bytes(&signed_value);
        vector::append(&mut self.buffer, bytes);
    }

    public fun push_decimal(self: &mut Hasher, value: &Decimal) {
        let (value, neg) = decimal::unpack(*value);
        push_i128(self, value, neg);
    }

    public fun push_decimal_le(self: &mut Hasher, value: &Decimal) {
        let (value, neg) = decimal::unpack(*value);
        push_i128_le(self, value, neg);
    }


    public fun push_bytes(self: &mut Hasher, bytes: vector<u8>) {
        vector::append(&mut self.buffer, bytes);
    }

    public fun generate_update_msg(
        value: &Decimal,
        queue_key: vector<u8>,
        feed_hash: vector<u8>,
        slothash: vector<u8>,
        max_variance: u64,
        min_responses: u32,
        timestamp: u64,
    ): vector<u8> {
        let hasher = new();
        assert!(vector::length(&queue_key) == 32, 1344);
        assert!(vector::length(&feed_hash) == 32, 1345);
        assert!(vector::length(&slothash) == 32, 1346);
        push_bytes(&mut hasher, queue_key);
        push_bytes(&mut hasher, feed_hash);
        push_decimal_le(&mut hasher, value);
        push_bytes(&mut hasher, slothash);
        push_u64_le(&mut hasher, max_variance);
        push_u32_le(&mut hasher, min_responses);
        push_u64_le(&mut hasher, timestamp);
        let Hasher { buffer } = hasher;
        buffer
    }

    public fun generate_attestation_msg(
        oracle_key: vector<u8>, 
        queue_key: vector<u8>,
        mr_enclave: vector<u8>,
        slothash: vector<u8>,
        secp256k1_key: vector<u8>,
        timestamp: u64,
    ): vector<u8> {
        let hasher = new();
        assert!(vector::length(&oracle_key) == 32, 1339);
        assert!(vector::length(&queue_key) == 32, 1340);
        assert!(vector::length(&mr_enclave) == 32, 1341);
        assert!(vector::length(&slothash) == 32, 1342);
        assert!(vector::length(&secp256k1_key) == 64, 1343);
        push_bytes(&mut hasher, oracle_key);
        push_bytes(&mut hasher, queue_key);
        push_bytes(&mut hasher, mr_enclave);
        push_bytes(&mut hasher, slothash);
        push_bytes(&mut hasher, secp256k1_key);
        push_u64_le(&mut hasher, timestamp);
        let Hasher { buffer } = hasher;
        buffer
    }

    public fun generate_update_hash(
        value: &Decimal,
        queue_key: vector<u8>,
        feed_hash: vector<u8>,
        slothash: vector<u8>,
        max_variance: u64,
        min_responses: u32,
        timestamp: u64,
    ): vector<u8> {
        let msg = generate_update_msg(
            value,
            queue_key,
            feed_hash,
            slothash,
            max_variance,
            min_responses,
            timestamp,
        );
        hash::sha2_256(msg)
    }

    public fun generate_v2_update_msg(
        slot: u64,
        timestamp: u64,
        feed_hashes: vector<vector<u8>>,
        values: vector<Decimal>,
        min_oracle_samples: vector<u8>,
    ): vector<u8> {
        let hasher = new();
        assert!(vector::length(&feed_hashes) == vector::length(&values), 1347);
        assert!(vector::length(&feed_hashes) == vector::length(&min_oracle_samples), 1348);

        push_u64_le(&mut hasher, slot);
        push_u64_le(&mut hasher, timestamp);

        let idx = 0;
        while (idx < vector::length(&feed_hashes)) {
            let feed_hash = *vector::borrow(&feed_hashes, idx);
            assert!(vector::length(&feed_hash) == 32, 1345);
            push_bytes(&mut hasher, feed_hash);
            push_decimal_le(&mut hasher, vector::borrow(&values, idx));
            push_u8(&mut hasher, *vector::borrow(&min_oracle_samples, idx));
            idx = idx + 1;
        };

        let Hasher { buffer } = hasher;
        buffer
    }

    public fun generate_v2_update_hash(
        slot: u64,
        timestamp: u64,
        feed_hashes: vector<vector<u8>>,
        values: vector<Decimal>,
        min_oracle_samples: vector<u8>,
    ): vector<u8> {
        let msg = generate_v2_update_msg(
            slot,
            timestamp,
            feed_hashes,
            values,
            min_oracle_samples,
        );
        hash::sha2_256(msg)
    }

    public fun generate_attestation_hash(
        oracle_key: vector<u8>, 
        queue_key: vector<u8>,
        mr_enclave: vector<u8>,
        slothash: vector<u8>,
        secp256k1_key: vector<u8>,
        timestamp: u64,
    ): vector<u8> {
        let msg = generate_attestation_msg(
            oracle_key,
            queue_key,
            mr_enclave,
            slothash,
            secp256k1_key,
            timestamp,
        );
        hash::sha2_256(msg)
    }

    public fun check_subvec(v1: &vector<u8>, v2: &vector<u8>, start_idx: u64): bool {
        if (vector::length(v1) < start_idx + vector::length(v2)) {
            return false
        };

        let iterations = vector::length(v2);
        while (iterations > 0) {
            let idx = iterations - 1;
            if (vector::borrow(v1, start_idx + idx) != vector::borrow(v2, idx)) {
                return false
            };
            iterations = iterations - 1;
        };

        true
    }

    #[test_only]
    use aptos_framework::secp256k1;
    #[test_only]
    use std::option;


    #[test_only]
    fun test_check_subvec(v1: &vector<u8>, v2: &vector<u8>, start_idx: u64) {
        let length_does_not_match: u64 = 1338;
        assert!(vector::length(v1) >= start_idx + vector::length(v2), length_does_not_match);
        let iterations = vector::length(v2);
        while (iterations > 0) {
            let idx = iterations - 1;
            assert!(vector::borrow(v1, start_idx + idx) == vector::borrow(v2, idx), idx as u64);
            iterations = iterations - 1;
        }
    }

    #[test]
    fun test_update_msg() { 
        let value = decimal::new(226943873990930561085963032052770576810, false);
        let queue_key = x"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let feed_hash = x"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let slothash = x"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
        let max_variance: u64 = 42;  
        let min_responses: u32 = 3;
        let timestamp: u64 = 1620000000;
        let value_num: u128 = 226943873990930561085963032052770576810;
        let msg = generate_update_msg(
            &value,
            queue_key,
            feed_hash,
            slothash,
            max_variance,
            min_responses,
            timestamp,
        );
        test_check_subvec(&msg, &queue_key, 0);
        test_check_subvec(&msg, &feed_hash, 32);
        test_check_subvec(&msg, &bcs::to_bytes(&value_num), 64);
        test_check_subvec(&msg, &slothash, 80);
        test_check_subvec(&msg, &bcs::to_bytes(&max_variance), 112);
        test_check_subvec(&msg, &bcs::to_bytes(&min_responses), 120);
        test_check_subvec(&msg, &bcs::to_bytes(&timestamp), 124);
    }

    #[test]
    fun test_attestation_msg() { 
        let oracle_key = x"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let queue_key = x"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let mr_enclave = x"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        let slothash = x"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd";
        let secp256k1_key = x"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
        let timestamp: u64 = 1620000000;
        let msg = generate_attestation_msg(
            oracle_key,
            queue_key,
            mr_enclave,
            slothash,
            secp256k1_key,
            timestamp,
        );
        test_check_subvec(&msg, &oracle_key, 0);
        test_check_subvec(&msg, &queue_key, 32);
        test_check_subvec(&msg, &mr_enclave, 64);
        test_check_subvec(&msg, &slothash, 96);
        test_check_subvec(&msg, &secp256k1_key, 128);
        test_check_subvec(&msg, &bcs::to_bytes(&timestamp), 192);
    }

    #[test]
    fun test_update_msg_ecrecover() { 
        let err: u64 = 1337;
        let value = decimal::new(88225582514986682302807, false);
        let queue_key = x"c9477bfb5ff1012859f336cf98725680e7705ba2abece17188cfb28ca66ca5b0";
        let feed_hash = x"2f24a24ce00a336bbf75f5e25086d32a6eb1d8717a013cf4f47610168405cd13";
        let slothash = x"0000000000000000000000000000000000000000000000000000000000000000";
        let max_variance: u64 = 1000000000;  
        let min_responses: u32 = 1;
        let timestamp: u64 = 1731467944;
        let signature = x"6cfbd56d878eb2e4ad74e27584a5ab99558f9092f0140fbeab43ced3680eff9e3853db14e03ed38679161b43aae490bb5a467617c620e531c0de50940cd93aa5";
        let msg = generate_update_msg(
            &value,
            queue_key,
            feed_hash,
            slothash,
            max_variance,
            min_responses,
            timestamp,
        );
  
        let hashed_msg = std::hash::sha2_256(msg);
        let signature = secp256k1::ecdsa_signature_from_bytes(signature);
        let recovered_pubkey = secp256k1::ecdsa_recover(
            hashed_msg, 
            1,
            &signature, 
        );
        assert!(option::is_some(&recovered_pubkey), err);
        let recovered_pubkey = secp256k1::ecdsa_raw_public_key_to_bytes(&option::extract(&mut recovered_pubkey));
        let expected_signer = x"072814bfdd26bcbeb9ecd2872f77b51012b11909726ce2ba64b3634f43d0ea12fa01e2fffe1c3b54305a83fe3365d4ee0579e98382ff9b4fb1e22baaee95dc7c";
        assert!(recovered_pubkey == expected_signer, err);
    }

    #[test]
    fun test_v2_update_msg_fixture() {
        let err: u64 = 1350;
        let feed_hashes = vector::empty<vector<u8>>();
        vector::push_back(
            &mut feed_hashes,
            x"1111111111111111111111111111111111111111111111111111111111111111",
        );
        vector::push_back(
            &mut feed_hashes,
            x"2222222222222222222222222222222222222222222222222222222222222222",
        );

        let values = vector::empty<Decimal>();
        vector::push_back(&mut values, decimal::new(123, false));
        vector::push_back(&mut values, decimal::new(456, true));

        let min_oracle_samples = vector::empty<u8>();
        vector::push_back(&mut min_oracle_samples, 1);
        vector::push_back(&mut min_oracle_samples, 3);

        let msg = generate_v2_update_msg(
            42,
            1234567890,
            feed_hashes,
            values,
            min_oracle_samples,
        );

        assert!(
            msg
                == x"2a00000000000000d20296490000000011111111111111111111111111111111111111111111111111111111111111117b00000000000000000000000000000001222222222222222222222222222222222222222222222222222222222222222238feffffffffffffffffffffffffffff03",
            err
        );
    }

    #[test]
    fun test_v2_update_hash_fixture() {
        let err: u64 = 1349;
        let feed_hashes = vector::empty<vector<u8>>();
        vector::push_back(
            &mut feed_hashes,
            x"1111111111111111111111111111111111111111111111111111111111111111",
        );
        vector::push_back(
            &mut feed_hashes,
            x"2222222222222222222222222222222222222222222222222222222222222222",
        );

        let values = vector::empty<Decimal>();
        vector::push_back(&mut values, decimal::new(123, false));
        vector::push_back(&mut values, decimal::new(456, true));

        let min_oracle_samples = vector::empty<u8>();
        vector::push_back(&mut min_oracle_samples, 1);
        vector::push_back(&mut min_oracle_samples, 3);

        let digest = generate_v2_update_hash(
            42,
            1234567890,
            feed_hashes,
            values,
            min_oracle_samples,
        );

        assert!(
            digest
                == x"b1150a89640f8091027fd408d50f666d4738872714d979f25b7bebec5db3f584",
            err
        );
    }
}
