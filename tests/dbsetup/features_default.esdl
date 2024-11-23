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