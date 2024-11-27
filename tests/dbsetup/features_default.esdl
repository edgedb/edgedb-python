type Child {
    required property num: int64 {
        constraint exclusive;
    }
};

type HasLinkPropsA {
    link child: Child {
        property a: str;
    }
};

type HasLinkPropsB {
    multi link children: Child {
        property b: str;
    }
};

type MultiProp {
    required property name: str;
    multi property tags: str;
};

type Theme {
    required property color: str;
    link branch: other::Branch {
        property note: str;
    }
};

type Foo {
    required property name: str;
};

type Bar {
    link foo: Foo;
    multi link many_foo: Foo;
    required property n: int64;
};

type Who {
    link foo: Foo;
    multi link many_foo: Foo {
        property note: str;
    };
    required property x: int64;
};
