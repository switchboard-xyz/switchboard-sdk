module switchboard::decimal {

    const DECIMALS: u8 = 18;


    struct Decimal has copy, drop, store { value: u128, neg: bool }
    const MAX_U128: u128 = 340282366920938463463374607431768211455;


    public fun zero(): Decimal {
        Decimal {
            value: 0,
            neg: false
        }
    }

    public fun new(value: u128, neg: bool): Decimal {
        Decimal { value, neg }
    }

    public fun unpack(self: Decimal): (u128, bool) {
        let Decimal { value, neg } = self;
        (value, neg)
    }

    public fun value(self: &Decimal): u128 {
        self.value
    }

    public fun dec(_: &Decimal): u8 {
        DECIMALS
    }

    public fun neg(self: &Decimal): bool {
        self.neg
    }

    public fun max_value(): Decimal {
        Decimal {
            value: MAX_U128,
            neg: false
        }
    }

    public fun equals(self: &Decimal, b: &Decimal): bool {
        self.value == b.value && self.neg == b.neg
    }

    public fun gt(self: &Decimal, b: &Decimal): bool {
        if (self.neg && b.neg) {
            return self.value < b.value
        } else if (self.neg) {
            return false
        } else if (b.neg) {
            return true
        };
        self.value > b.value
    }

    public fun lt(self: &Decimal, b: &Decimal): bool {
        if (self.neg && b.neg) {
            return self.value > b.value
        } else if (self.neg) {
            return true
        } else if (b.neg) {
            return false
        };
        self.value < b.value
    }

    public fun add(self: &Decimal, b: &Decimal): Decimal {
        // -x + -y
        if (self.neg && b.neg) {
            let sum = add_internal(self, b);
            sum.neg = true;
            sum
        // -x + y
        } else if (self.neg) {
            sub_internal(b, self)

        // x + -y
        } else if (b.neg) {
            sub_internal(self, b)

        // x + y
        } else {
            add_internal(self, b)
        }
    }

    public fun sub(self: &Decimal, b: &Decimal): Decimal {
        // -x - -y
        if (self.neg && b.neg) {
            sub_internal(b, self)
        // -x - y
        } else if (self.neg) {
            let sum = add_internal(self, b);
            sum.neg = true;
            sum
        // x - -y
        } else if (b.neg) {
            add_internal(self, b)
        // x - y
        } else {
            sub_internal(self, b)
        }
    }

    public fun min(self: &Decimal, b: &Decimal): Decimal {
        if (lt(self, b)) {
            *self
        } else {
            *b
        }
    }

    public fun max(self: &Decimal, b: &Decimal): Decimal {
        if (gt(self, b)) {
            *self
        } else {
            *b
        }
    }


    fun add_internal(self: &Decimal, b: &Decimal): Decimal {
        new(self.value + b.value, false)
    }

    fun sub_internal(self: &Decimal, b: &Decimal): Decimal {
        if (b.value > self.value) {
            new(b.value - self.value, true)
        } else {
            new(self.value - b.value, false)
        }
    }

    public fun scale_to_decimals(num: &Decimal, current_decimals: u8): u128 {
        if (current_decimals < DECIMALS) {
            return (num.value * pow_10(DECIMALS - current_decimals))
        } else {
            return (num.value / pow_10(current_decimals - DECIMALS))
        }
    }


    public fun pow_10(e: u8): u128 {
        let i = 0;
        let result = 1;
        while (i < e) {
            result = result * 10;
            i = i + 1;
        };
        result
    }
}