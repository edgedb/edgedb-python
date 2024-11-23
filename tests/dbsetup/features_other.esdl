type Branch {
    required property val: str {
        constraint exclusive;
    }

    multi link leaves: other::nested::Leaf;
};
