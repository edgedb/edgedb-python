insert Child {num := 0};
insert Child {num := 1};

insert HasLinkPropsA {
    child := (select Child{@a := 'single'} filter .num = 0)
};

insert HasLinkPropsB;
update HasLinkPropsB
set {
    children += (select Child{@b := 'hello'} filter .num = 0)
};
update HasLinkPropsB
set {
    children += (select Child{@b := 'world'} filter .num = 1)
};

insert MultiProp {
    name := 'got one',
    tags := {'solo tag'},
};

insert MultiProp {
    name := 'got many',
    tags := {'one', 'two', 'three'},
};

insert other::nested::Leaf {
    num := 10
};

insert other::nested::Leaf {
    num := 20
};

insert other::nested::Leaf {
    num := 30
};

insert other::Branch {
    val := 'big',
    leaves := (select other::nested::Leaf filter .num != 10),
};

insert other::Branch {
    val := 'small',
    leaves := (select other::nested::Leaf filter .num = 10),
};

insert Theme {
    color := 'green',
    branch := (
        select other::Branch{@note := 'fresh'} filter .val = 'big'
    )
};

insert Theme {
    color := 'orange',
    branch := (
        select other::Branch{@note := 'fall'} filter .val = 'big'
    )
};