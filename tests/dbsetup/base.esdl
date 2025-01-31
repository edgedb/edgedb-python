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

type User extending Named {
    # test computed backlink
    groups := .<users[is UserGroup];
}

type Post {
    required body: str;
    required link author: User;
}

type AssortedScalars {
    required name: str;
    vals: array<str>;

    date: cal::local_date;
    time: cal::local_time;
    ts: datetime;
    lts: cal::local_datetime;
    bstr: bytes;
}