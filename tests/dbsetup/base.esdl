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
