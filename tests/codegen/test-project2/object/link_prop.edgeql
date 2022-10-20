create type Person {
    create required property name -> str;
    create property created_at -> datetime;
    create multi link friends -> Person {
        create property strength -> float64;
        create property created_at -> datetime;
    }
};

select Person {
    name,
    friends: {
        name,
        @created_at,
        created_at,
        @strength,
    }
};
