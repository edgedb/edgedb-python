abstract type Named {
    required name: str;
}

type UserGroup extending Named {
    # many-to-many
    multi link users: User;
}

type GameSession {
    required num: int64;
    # one-to-many
    multi link players: User {
        constraint exclusive;
    };
}

type User extending Named;

type Post {
    required body: str;
    required link author: User;
}

type Child {
    required property num: int64 {
        constraint exclusive;
    }
}

type HasLinkPropsA {
    link child: Child {
        property a: str;
    }
}

type HasLinkPropsB {
    multi link children: Child {
        property b: str;
    }
}