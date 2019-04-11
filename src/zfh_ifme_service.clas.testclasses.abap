INTERFACE lif_unit_test.
ENDINTERFACE.
CLASS zfh_ifme_service DEFINITION LOCAL FRIENDS lif_unit_test.


CLASS ltc_data_driven_test DEFINITION
  INHERITING FROM cl_ifme_test_base
  RISK LEVEL HARMLESS
  DURATION SHORT
  FINAL
  FOR TESTING.

  PUBLIC SECTION.
    INTERFACES: lif_unit_test.

  PRIVATE SECTION.
    CLASS-DATA:
      incoming_service TYPE REF TO zfh_ifme_service,
      tdc_container       TYPE REF TO cl_ifme_tdc_db_e,
      fmt_vers_key     TYPE ifme_pk_fmtvers.

    DATA:
      db_reader        TYPE REF TO if_ifme_db_reader,
      test_container_name TYPE etobj_name VALUE 'IFME_TDC_FORMATS_MOCKDB'.

    CLASS-METHODS:
      class_teardown,
      class_setup.

    METHODS:
      setup RAISING cx_ecatt_tdc_access,

      create_mapping_format_flat FOR TESTING RAISING cx_static_check,
      create_mapping_format_xml FOR TESTING RAISING cx_static_check,
      copy_mapping_format_top_level FOR TESTING RAISING cx_static_check,
      copy_mapping_format_as_child FOR TESTING RAISING cx_static_check,
      copy_mapping_format_sibling FOR TESTING RAISING cx_static_check,
      delete_mapping_format FOR TESTING RAISING cx_static_check,
      delete_mapping_format_parent FOR TESTING RAISING cx_static_check,
      delete_mapping_format_wrong FOR TESTING RAISING cx_static_check,
      convert_messages_to_entity FOR TESTING RAISING cx_static_check,
      convert_mssgs_to_entity_fail FOR TESTING RAISING cx_static_check,
      get_format_structure FOR TESTING RAISING cx_static_check,
      get_draft_processor_e FOR TESTING RAISING cx_static_check,
      get_draft_processor_c FOR TESTING RAISING cx_static_check,
      get_initial_backup_version_num FOR TESTING RAISING cx_static_check,
      get_backup_version_num FOR TESTING RAISING cx_static_check,
      create_maintenance_version FOR TESTING RAISING cx_static_check,
      export_mapping_format FOR TESTING RAISING cx_static_check,
      export_mapping_format_fail FOR TESTING RAISING cx_static_check,
      import_mapping_format FOR TESTING RAISING cx_static_check,
      create_backup_version FOR TESTING RAISING cx_static_check
      .

ENDCLASS.

CLASS ltc_data_driven_test IMPLEMENTATION.

  METHOD class_setup.
    tdc_container = NEW cl_ifme_tdc_db_e( ).
    cl_ifme_db_service_factory=>get_instance( )->set_system_info_provider( NEW cl_ifme_system_info_mock( ) ).
    incoming_service = NEW #( validator = NEW #( NEW cl_ifme_system_info_mock( ) ) ).
    fmt_vers_key = VALUE ifme_pk_fmtvers( hierarchytree = 'MT300' hierarchytreetype = 'TRMC' hierarchytreeversion = 'MAINT').
*    incoming_service->transport_service =
  ENDMETHOD.

  METHOD class_teardown.
    tdc_container->destroy_mock_db( ).
  ENDMETHOD.

  METHOD setup.
    tdc_container->clear_mock_db( ).
  ENDMETHOD.

  METHOD create_mapping_format_flat.
    "Given
    tdc_container->initialize_mock_db( container = test_container_name variant = 'EMPTYDB' ).

    "When
    DATA(result) = incoming_service->create_mapping_format(
                                        hierarchytree = fmt_vers_key-hierarchytree
                                        hierarchytreetype = fmt_vers_key-hierarchytreetype
                                        file_format = cl_ifme_inputfileformat=>flat
                                        description = CONV #( fmt_vers_key-hierarchytree )
                                      ).

    DATA(actual_format) = cl_ifme_db_service_factory=>get_instance( )->get_db_reader( fmt_vers_key-hierarchytree )->get_format( fmt_vers_key ).

    DATA(expected_format) = tdc_container->get_format_from_tdc_mock_db(
      container = test_container_name
      variant = 'FORMAT_NEW_FLAT'
    ).

    "Then
    assert_containers_data_equal( CHANGING expected = expected_format actual = actual_format ).
  ENDMETHOD.

  METHOD create_mapping_format_xml.
    "Given
    tdc_container->initialize_mock_db( container = test_container_name variant = 'EMPTYDB' ).

    "When
    DATA(result) = incoming_service->create_mapping_format(
                                        hierarchytree = fmt_vers_key-hierarchytree
                                        hierarchytreetype = fmt_vers_key-hierarchytreetype
                                        file_format = cl_ifme_inputfileformat=>xml
                                        description = CONV #( fmt_vers_key-hierarchytree )
                                      ).

    DATA(actual_format) = cl_ifme_db_service_factory=>get_instance( )->get_db_reader( fmt_vers_key-hierarchytree )->get_format( fmt_vers_key ).

    DATA(expected_format) = tdc_container->get_format_from_tdc_mock_db(
      container = test_container_name
      variant = 'FORMAT_NEW_XML'
    ).

    "Then
    assert_containers_data_equal( CHANGING expected = expected_format actual = actual_format ).
  ENDMETHOD.

  METHOD copy_mapping_format_as_child.
    DATA fmt_vers_new TYPE ifme_pk_fmtvers.

    "Given
    fmt_vers_new = VALUE #( hierarchytree = 'MT300_CHILD' hierarchytreetype = 'TRMC' hierarchytreeversion = 'MAINT' ).
    tdc_container->initialize_mock_db( container = test_container_name variant = 'BASEDB' ).

    "When
    DATA(result) = incoming_service->copy_mapping_format(
                                        sourcehierarchytree = fmt_vers_key-hierarchytree
                                        sourcehierarchytreetype = fmt_vers_key-hierarchytreetype
                                        hierarchytree = fmt_vers_new-hierarchytree
                                        copy_as_child = abap_true
                                      ).

    DATA(actual_format) = cl_ifme_db_service_factory=>get_instance( )->get_db_reader( fmt_vers_new-hierarchytree )->get_format( fmt_vers_new ).

    DATA(expected_format) = tdc_container->get_format_from_tdc_mock_db(
      container = test_container_name
      variant = 'FORMAT_COPY_CHILD'
    ).

    "Then
    assert_containers_data_equal( CHANGING expected = expected_format actual = actual_format ).
  ENDMETHOD.

  METHOD copy_mapping_format_sibling.
    DATA:
      fmt_vers_new    TYPE ifme_pk_fmtvers,
      fmt_vers_parent TYPE ifme_pk_fmtvers.

    "Given
    fmt_vers_parent = VALUE #( hierarchytree = 'MT300_001' hierarchytreetype = 'TRMC' hierarchytreeversion = 'MAINT' ).
    fmt_vers_new = VALUE #( hierarchytree = 'MT300_SIBLING' hierarchytreetype = 'TRMC' hierarchytreeversion = 'MAINT' ).
    tdc_container->initialize_mock_db( container = test_container_name variant = 'BASEDB' ).

    "When
    DATA(result) = incoming_service->copy_mapping_format(
                                        sourcehierarchytree = fmt_vers_parent-hierarchytree
                                        sourcehierarchytreetype = fmt_vers_parent-hierarchytreetype
                                        hierarchytree = fmt_vers_new-hierarchytree
                                        copy_as_child = abap_false
                                      ).

    DATA(actual_format) = cl_ifme_db_service_factory=>get_instance( )->get_db_reader( fmt_vers_new-hierarchytree )->get_format( fmt_vers_new ).

    DATA(expected_format) = tdc_container->get_format_from_tdc_mock_db(
      container = test_container_name
      variant = 'FORMAT_COPY_SIBLING'
    ).

    "Then
    assert_containers_data_equal( CHANGING expected = expected_format actual = actual_format ).
  ENDMETHOD.

  METHOD copy_mapping_format_top_level.
    DATA fmt_vers_new TYPE ifme_pk_fmtvers.

    "Given
    fmt_vers_new = VALUE #( hierarchytree = 'MT300_COPY' hierarchytreetype = 'TRMC' hierarchytreeversion = 'MAINT' ).
    tdc_container->initialize_mock_db( container = test_container_name variant = 'BASEDB' ).

    "When
    DATA(result) = incoming_service->copy_mapping_format(
                                        sourcehierarchytree = fmt_vers_key-hierarchytree
                                        sourcehierarchytreetype = fmt_vers_key-hierarchytreetype
                                        hierarchytree = fmt_vers_new-hierarchytree
                                        copy_as_child = abap_false
                                      ).

    DATA(actual_format) = cl_ifme_db_service_factory=>get_instance( )->get_db_reader( fmt_vers_new-hierarchytree )->get_format( fmt_vers_new ).

    DATA(expected_format) = tdc_container->get_format_from_tdc_mock_db(
      container = test_container_name
      variant = 'FORMAT_COPY_TOP'
    ).

    "Then
    assert_containers_data_equal( CHANGING expected = expected_format actual = actual_format ).
  ENDMETHOD.

  METHOD convert_messages_to_entity.

    "Given
    TYPES:
      BEGIN OF s_funtion_return,
        hierarchytree TYPE ifme_hierarchytree,
        messagetype   TYPE c LENGTH 1,
        message       TYPE string,
      END OF s_funtion_return.
    DATA: messages TYPE bapiret2_tab,
          result_entity   TYPE s_funtion_return.

    INSERT VALUE #( type = 'E' message = 'text' ) INTO TABLE messages.

    "When
    incoming_service->convert_messages_to_entity(
                        EXPORTING to_convert = messages
                        IMPORTING entity = result_entity
                      ).
    "Then
    assert_true( COND #( WHEN result_entity IS INITIAL THEN abap_false ELSE abap_true ) ).
  ENDMETHOD.

  METHOD convert_mssgs_to_entity_fail.

    "Given
    TYPES:
      BEGIN OF s_funtion_return,
        hierarchytree TYPE ifme_hierarchytree,
        messagetype   TYPE c LENGTH 1,
        messagetext       TYPE string,
      END OF s_funtion_return.
    DATA: messages TYPE bapiret2_tab,
          result_entity   TYPE s_funtion_return.

    INSERT VALUE #( type = 'E' message = 'text' ) INTO TABLE messages.

    "When
    incoming_service->convert_messages_to_entity(
                        EXPORTING to_convert = messages
                        IMPORTING entity = result_entity
                      ).
    "Then
    assert_false( COND #( WHEN result_entity IS INITIAL THEN abap_false ELSE abap_true ) ).
  ENDMETHOD.

  METHOD get_format_structure.

    TEST-INJECTION get_format_structure.
      ls_ifmefmttype-outputstructurename = 'TCORS_SWIFT'.
    END-TEST-INJECTION.

    incoming_service->get_format_structure(
        EXPORTING iv_hierarchytreetype = 'TRMC'
        IMPORTING et_fields            = DATA(lt_fields) ).

    assert_not_initial( lt_fields[ 1 ] ).

  ENDMETHOD.

  METHOD delete_mapping_format.
     DATA: fmt_delete TYPE ifme_pk_fmt.

    "Given
    fmt_delete = VALUE #( hierarchytree = 'MT300_001' hierarchytreetype = 'TRMC' ).
    tdc_container->initialize_mock_db( container = test_container_name variant = 'BASEDB' ).

    "When
    DATA(result) = incoming_service->delete_mapping_format(
                                        hierarchytree = fmt_delete-hierarchytree
                                        hierarchytreetype = fmt_delete-hierarchytreetype
                                        deletedraft = abap_true
                                      ).
    DATA(all_format_versions) = cl_ifme_db_service_factory=>get_instance( )->get_db_reader( fmt_delete-hierarchytree )->get_format_versions( fmt_delete ).

    "Then
    assert_initial( act = all_format_versions ).
    assert_initial( act = result ).
  ENDMETHOD.

  METHOD delete_mapping_format_parent.
     DATA: fmt_delete TYPE ifme_pk_fmt.

    "Given
    fmt_delete = VALUE #( hierarchytree = 'MT300' hierarchytreetype = 'TRMC' ).
    tdc_container->initialize_mock_db( container = test_container_name variant = 'BASEDB' ).

    "When
    DATA(result) = incoming_service->delete_mapping_format(
                                        hierarchytree = fmt_delete-hierarchytree
                                        hierarchytreetype = fmt_delete-hierarchytreetype
                                        deletedraft = abap_true
                                      ).

    "Then
    assert_true( COND #( WHEN result IS NOT INITIAL THEN abap_true ELSE abap_false ) ).
  ENDMETHOD.



  METHOD delete_mapping_format_wrong.
     DATA: fmt_delete TYPE ifme_pk_fmt.

    "Given
    fmt_delete = VALUE #( hierarchytree = 'NOFORMAT' hierarchytreetype = 'TRMC' ).
    tdc_container->initialize_mock_db( container = test_container_name variant = 'BASEDB' ).

    "When
    DATA(result) = incoming_service->delete_mapping_format(
                                        hierarchytree = fmt_delete-hierarchytree
                                        hierarchytreetype = fmt_delete-hierarchytreetype
                                        deletedraft = abap_true
                                      ).

    "Then
    assert_true( COND #( WHEN result IS NOT INITIAL THEN abap_true ELSE abap_false ) ).
  ENDMETHOD.

  METHOD get_draft_processor_e.
    "Given
      DATA: success TYPE abap_bool.
    "When
    DATA(act) = incoming_service->get_draft_processor( abap_false ).
    "Then
    IF act IS INSTANCE OF cl_inc_format_draft_proc_e.
      success = abap_true.
    ENDIF.
    assert_true( success ).
  ENDMETHOD.

  METHOD get_draft_processor_c.
    "Given
      DATA: success TYPE abap_bool.
    "When
    DATA(act) = incoming_service->get_draft_processor( abap_true ).
    "Then
    IF act IS INSTANCE OF cl_inc_format_draft_proc_c.
      success = abap_true.
    ENDIF.
    assert_true( success ).
  ENDMETHOD.

  METHOD get_backup_version_num.
    "Given
    DATA(fmt_key) = VALUE ifme_pk_fmt( hierarchytree = 'MT300_001' hierarchytreetype = 'TRMC' ).
    tdc_container->initialize_mock_db( container = test_container_name variant = 'BASEDB' ).

    "When
    DATA(result) = incoming_service->get_backup_version_number( fmt_key ).

    "Then
    assert_equals( exp = 3 act = result ).
  ENDMETHOD.

  METHOD get_initial_backup_version_num.
    "Given
    tdc_container->initialize_mock_db( container = test_container_name variant = 'BASEDB' ).

    "When
    DATA(result) = incoming_service->get_backup_version_number( VALUE #( hierarchytree = fmt_vers_key-hierarchytree hierarchytreetype = fmt_vers_key-hierarchytreetype ) ).

    "Then
    assert_equals( exp = 1 act = result ).
  ENDMETHOD.

  METHOD create_maintenance_version.

    "Given
    tdc_container->initialize_mock_db( container = test_container_name variant = 'BASEDB' ).
    DATA(fmt_vers_maint) = VALUE ifme_pk_fmtvers( hierarchytree = 'MT302' hierarchytreetype = 'TRMC' hierarchytreeversion = cl_ifme_hierarchytreeversion=>maint ).

    "When
    DATA(result) = incoming_service->create_maintenance( hierarchytree = fmt_vers_maint-hierarchytree hierarchytreetype = fmt_vers_maint-hierarchytreetype ).

    DATA(actual_format) = cl_ifme_db_service_factory=>get_instance( )->get_db_reader( fmt_vers_maint-hierarchytree )->get_format( fmt_vers_maint ).

    DATA(expected_format) = tdc_container->get_format_from_tdc_mock_db(
      container = test_container_name
      variant = 'FORMAT_CREATE_MAINT'
    ).

    "Then
    assert_containers_data_equal( CHANGING expected = expected_format actual = actual_format ).
    assert_initial( act = result ).
  ENDMETHOD.

  METHOD export_mapping_format.
    tdc_container->initialize_mock_db( container = test_container_name variant = 'IMPORTDB' ).

    incoming_service->export_mapping_format( EXPORTING hierarchytree = 'MT300'
                                                       hierarchytreetype = 'TRMC'
                                             IMPORTING result = DATA(result) ).

    assert_equals( act = xsdbool( line_exists( result[ type = 'E' ] ) ) exp = abap_false ).
  ENDMETHOD.

  METHOD export_mapping_format_fail.
    tdc_container->initialize_mock_db( container = test_container_name variant = 'IMPORTDB' ).

    incoming_service->export_mapping_format( EXPORTING hierarchytree = 'Invalid'
                                                       hierarchytreetype = 'TRMC'
                                             IMPORTING result = DATA(result) ).

    assert_equals( act = xsdbool( line_exists( result[ type = 'E' ] ) ) exp = abap_true ).
  ENDMETHOD.

  METHOD import_mapping_format.
    tdc_container->initialize_mock_db( container = test_container_name variant = 'IMPORTDB' ).

    incoming_service->export_mapping_format( EXPORTING hierarchytree = 'MT300'
                                                       hierarchytreetype = 'TRMC'
                                             IMPORTING xml_xstring = DATA(xml_string) ).

    DATA(result) = incoming_service->import_mapping_format( hierarchytree = 'MT300'
                                                            hierarchytreetype = 'TRMC'
                                                            xml_xstring = xml_string
                                                            delete_draft = abap_false ).

    assert_equals( act = xsdbool( line_exists( result[ type = 'E' ] ) ) exp = abap_false ).
  ENDMETHOD.

  METHOD create_backup_version.
    "Given
    tdc_container->initialize_mock_db( container = test_container_name variant = 'BASEDB' ).
    DATA(fmt_vers_active) = fmt_vers_key.
    fmt_vers_active-hierarchytreeversion = cl_ifme_hierarchytreeversion=>active.

    DATA(active_format) = cl_ifme_db_service_factory=>get_instance( )->get_db_reader( fmt_vers_active-hierarchytree )->get_format( fmt_vers_active ).

    "When
    incoming_service->backup_version( active_format ).
    DATA(fmt_vers_backup) = fmt_vers_active.
    fmt_vers_backup-hierarchytreeversion = '000001'.
    DATA(actual_backup_format) = cl_ifme_db_service_factory=>get_instance( )->get_db_reader( fmt_vers_backup-hierarchytree )->get_format( fmt_vers_backup ).
    DATA(expected_format) = tdc_container->get_format_from_tdc_mock_db(
      container = test_container_name
      variant = 'FORMAT_BACKUP'
    ).

    "Then
    assert_containers_data_equal( CHANGING expected = expected_format actual = actual_backup_format ).
  ENDMETHOD.

ENDCLASS.
