CLASS zfh_test_diff DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.
    methods test.
  PROTECTED SECTION.
ENDCLASS.



CLASS zfh_test_diff IMPLEMENTATION.

  METHOD test.
    write: `changed`.
    write: `added`.
  ENDMETHOD.

ENDCLASS.
