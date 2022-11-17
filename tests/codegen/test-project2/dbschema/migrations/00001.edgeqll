CREATE MIGRATION m1fqtauhtvc2w56wh2676x5g26aye22ghx7b7mtnbfekxpmxmnjx2a
    ONTO initial
{
  CREATE TYPE default::Person {
      CREATE MULTI LINK friends -> default::Person {
          CREATE PROPERTY strength -> std::float64;
      };
      CREATE REQUIRED PROPERTY name -> std::str {
          CREATE CONSTRAINT std::exclusive;
      };
  };
};
