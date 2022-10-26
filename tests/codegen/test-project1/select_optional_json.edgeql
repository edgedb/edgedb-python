create type TestCase {
    create link snake_case -> TestCase;
};

select (<optional json>$0, TestCase {snake_case});
