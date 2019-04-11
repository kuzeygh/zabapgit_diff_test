CLASS zfh_ifme_service DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.

    INTERFACES:
      if_ifme_service.

    ALIASES:
      create_mapping_format   FOR if_ifme_service~create_mapping_format,
      create_maintenance      FOR if_ifme_service~create_maintenance,
      copy_mapping_format     FOR if_ifme_service~copy_mapping_format,
      delete_mapping_format   FOR if_ifme_service~delete_mapping_format,
      activate_mapping_format FOR if_ifme_service~activate_mapping_format,
      check_mapping_format    FOR if_ifme_service~check_mapping_format,
      export_mapping_format   FOR if_ifme_service~export_mapping_format,
      import_mapping_format   FOR if_ifme_service~import_mapping_format,

      convert_messages_to_entity FOR if_ifme_service~convert_messages_to_entity,
      get_format_structure       FOR if_ifme_service~get_format_structure,
      get_transport_request      FOR if_ifme_service~get_transport_request.

    METHODS constructor
      IMPORTING
        !validator          TYPE REF TO cl_ifme_service_validator OPTIONAL
        !db_service_factory TYPE REF TO cl_ifme_db_service_factory OPTIONAL.


  PROTECTED SECTION.
  PRIVATE SECTION.

    DATA validator TYPE REF TO cl_ifme_service_validator .
    DATA transport_service TYPE REF TO if_ifme_transport_service .

    METHODS initialize_mapping_format
      IMPORTING
        !hierarchytreetype TYPE ifme_hierarchytreetype
        !hierarchytree     TYPE ifme_hierarchytree
        !description       TYPE ifme_shorttext
        !file_format       TYPE ifme_inputfileformat
      RETURNING
        VALUE(format_data) TYPE REF TO cl_ifme_db_format_version_data .
    METHODS copy_format_version_data
      IMPORTING
        !source_format       TYPE REF TO cl_ifme_db_format_version_data
        !copy_as_child       TYPE abap_bool
      RETURNING
        VALUE(target_format) TYPE REF TO cl_ifme_db_format_version_data .
    METHODS copy_format_subtable
      IMPORTING
        !from_table      TYPE ANY TABLE
        !copy_as_child   TYPE abap_bool
        !copy_as_sibling TYPE abap_bool
      EXPORTING
        !to_table        TYPE STANDARD TABLE .
    METHODS append_parent_data
      IMPORTING
        !tree_data   TYPE REF TO cl_ifme_db_format_version_data
        !p_tree_data TYPE REF TO cl_ifme_db_format_version_data .
    METHODS update_timestamp
      IMPORTING
        !mapping_format TYPE REF TO cl_ifme_db_format_version_data .
    METHODS convert_message
      RETURNING
        VALUE(result) TYPE bapiret2_tab .
    METHODS get_draft_format_data
      IMPORTING
        !is_customer             TYPE abap_bool
        !draft_root_instance_key TYPE /bobf/conf_key
      RETURNING
        VALUE(result)            TYPE REF TO cl_ifme_db_format_version_data
      RAISING
        /bobf/cx_frw .
    METHODS get_draft_processor
      IMPORTING
        !is_customer  TYPE abap_bool
      RETURNING
        VALUE(result) TYPE REF TO if_inc_format_draft_proc
      RAISING
        /bobf/cx_frw .
    METHODS save_transport
      IMPORTING
        !format_key TYPE ifme_pk_fmt
        !request    TYPE trkorr OPTIONAL .
    METHODS backup_version
      IMPORTING
        !version_data TYPE REF TO cl_ifme_db_format_version_data .
    METHODS get_backup_version_number
      IMPORTING
        !format_key           TYPE ifme_pk_fmt
      RETURNING
        VALUE(version_number) TYPE ifme_hierarchytreeversion .
    METHODS delete_draft
      IMPORTING draft_uuid TYPE /bobf/uuid
                draft_proc_manager TYPE REF TO if_inc_format_draft_proc
      RETURNING VALUE(result) TYPE bapiret2_tab .
ENDCLASS.



CLASS zfh_ifme_service IMPLEMENTATION.


  METHOD activate_mapping_format.
    DATA: draft_processor TYPE REF TO if_inc_format_draft_proc.

    TRY.
        DATA(format_draft_data) = get_draft_format_data( is_customer = is_customer draft_root_instance_key = draft_root_instance_key ).
        draft_processor = get_draft_processor( is_customer ).
      CATCH /bobf/cx_frw.
        MESSAGE e004 INTO DATA(msg).
        result = convert_message( ).
        RETURN.
    ENDTRY.

    result = validator->validate_format_draft_data( format_draft_data ).
    CHECK result IS INITIAL.

    result = validator->validate_authority_check( activity = cl_ifme_authority_check=>c_auth_actvt_change hierarchytreetype = format_draft_data->data-version-hierarchytreetype ).
    CHECK result IS INITIAL.

    DATA(tree_builder) = NEW cl_ifme_tree_builder( ).
    DATA(tree_checker) = NEW cl_ifme_tree_checker( tree_builder->build( format_draft_data ) ).

    result = tree_checker->execute( ).
    CHECK xsdbool( line_exists( result[ type = 'E' ] ) ) = abap_false.

    DATA(format_key) = CORRESPONDING ifme_pk_fmtvers( format_draft_data->data-version ).
    GET TIME STAMP FIELD DATA(ts).

    " save the maint version to the database
    format_draft_data->data-version-lastchangedatetime = ts.
    format_draft_data->data-version-lastchangebyuser = sy-uname.
    format_draft_data->data-version-datareleaserequestid = me->transport_service->translate_requestid( request ).

    IF format_draft_data->data-format-hierarchyparenttree IS NOT INITIAL.
      append_parent_data( tree_data = format_draft_data p_tree_data = tree_builder->build_tree( tree_key = format_key )->version_data ).
    ENDIF.

    TRY.
        DATA(db_service) = cl_ifme_db_service_factory=>get_instance( )->get_db_writer( format_key-hierarchytree ).
        db_service->create_version( format_draft_data ).
      CATCH cx_ifme_db.
        MESSAGE e005 INTO msg.
        result = convert_message( ).
        RETURN.
    ENDTRY.

    "Backup last active version
    "Check whether assigned request for active version is the same = Open status
    DATA(lr_last_active_version) = cl_ifme_db_service_factory=>get_instance( )->get_db_reader( format_key-hierarchytree )->get_format(
      VALUE #( hierarchytreetype    = format_key-hierarchytreetype
               hierarchytree        = format_key-hierarchytree
               hierarchytreeversion = cl_ifme_hierarchytreeversion=>active ) ).

    IF lr_last_active_version->data-version-datareleaserequestid IS NOT INITIAL AND
       lr_last_active_version->data-version-datareleaserequestid NE format_draft_data->data-version-datareleaserequestid.

      backup_version( lr_last_active_version ).
    ENDIF.


    " save the active version to the database
    format_key-hierarchytreeversion = cl_ifme_hierarchytreeversion=>active.
    cl_ifme_service_common=>update_format_key( new_format_key = format_key
                                               mapping_format = format_draft_data ).

    TRY.
        db_service->delete_version( VALUE ifme_pk_fmtvers( hierarchytree = format_key-hierarchytree hierarchytreetype = format_key-hierarchytreetype hierarchytreeversion = cl_ifme_hierarchytreeversion=>active ) ).
        db_service->create_version( format_draft_data ).
      CATCH cx_ifme_db.
        MESSAGE e005 INTO msg.
        result = convert_message( ).
        ROLLBACK WORK.
        RETURN.
    ENDTRY.

    TRY.
        save_transport( format_key = CORRESPONDING ifme_pk_fmt( format_key ) request = request ).
      CATCH cx_ifme_service.
        result = convert_message( ).
        ROLLBACK WORK.
        RETURN.
    ENDTRY.

    " delete draft
    draft_processor->delete( VALUE #( (  key =  draft_root_instance_key ) ) ).
    cl_inc_format_bo_proc_utils=>get_transaction_manager( )->save( ).

    " USAGE MEASUREMENT
    CASE format_draft_data->data-version-hierarchytreetype.
      WHEN cl_ifme_hierarchytreetype=>payment.
        cl_ifme_usage_measurement=>insert_measurement( cl_ifme_usage_measurement=>paym_activate_mapping ).
      WHEN cl_ifme_hierarchytreetype=>treasury.
        cl_ifme_usage_measurement=>insert_measurement( cl_ifme_usage_measurement=>trmc_activate_mapping ).
    ENDCASE.
  ENDMETHOD.


  METHOD append_parent_data.
    LOOP AT p_tree_data->data-flat_attributes INTO DATA(flat_attributes) WHERE itemtype = cl_ifme_itemtype=>parent.
      INSERT flat_attributes INTO TABLE tree_data->data-flat_attributes.
    ENDLOOP.

    LOOP AT p_tree_data->data-constants INTO DATA(constants) WHERE itemtype = cl_ifme_itemtype=>parent.
      INSERT constants INTO TABLE tree_data->data-constants.
    ENDLOOP.

    LOOP AT p_tree_data->data-enums INTO DATA(enums) WHERE itemtype = cl_ifme_itemtype=>parent.
      INSERT enums INTO TABLE tree_data->data-enums.
    ENDLOOP.

    LOOP AT p_tree_data->data-enums_v INTO DATA(enums_v) WHERE itemtype = cl_ifme_itemtype=>parent.
      INSERT enums_v INTO TABLE tree_data->data-enums_v.
    ENDLOOP.

    LOOP AT p_tree_data->data-nodes INTO DATA(nodes) WHERE itemtype = cl_ifme_itemtype=>parent.
      INSERT nodes INTO TABLE tree_data->data-nodes.
    ENDLOOP.

    LOOP AT p_tree_data->data-fields INTO DATA(fields) WHERE itemtype = cl_ifme_itemtype=>parent.
      INSERT fields INTO TABLE tree_data->data-fields.
    ENDLOOP.

    LOOP AT p_tree_data->data-field_groups INTO DATA(field_groups) WHERE itemtype = cl_ifme_itemtype=>parent.
      INSERT field_groups INTO TABLE tree_data->data-field_groups.
    ENDLOOP.

    LOOP AT p_tree_data->data-records INTO DATA(records) WHERE itemtype = cl_ifme_itemtype=>parent.
      INSERT records INTO TABLE tree_data->data-records.
    ENDLOOP.

    LOOP AT p_tree_data->data-record_groups INTO DATA(record_groups) WHERE itemtype = cl_ifme_itemtype=>parent.
      INSERT record_groups INTO TABLE tree_data->data-record_groups.
    ENDLOOP.

  ENDMETHOD.


  METHOD backup_version.

    DATA: ls_version_key TYPE ifme_pk_fmtvers.

    TRY.
        DATA(db_service) = cl_ifme_db_service_factory=>get_instance( )->get_db_writer( version_data->data-version-hierarchytree ).

        ls_version_key-hierarchytreetype    = version_data->data-version-hierarchytreetype.
        ls_version_key-hierarchytree        = version_data->data-version-hierarchytree.
        ls_version_key-hierarchytreeversion = get_backup_version_number( CORRESPONDING ifme_pk_fmt( ls_version_key ) ).

        cl_ifme_service_common=>update_format_key( new_format_key = ls_version_key
                                                   mapping_format = version_data ).

        db_service->create_version( version_data ).

      CATCH cx_ifme_db.
        RAISE EXCEPTION TYPE cx_ifme_service.
    ENDTRY.

  ENDMETHOD.


  METHOD check_mapping_format.

    TRY.
        DATA(tree_data) = get_draft_format_data( is_customer = is_customer draft_root_instance_key = draft_root_instance_key ).
      CATCH /bobf/cx_frw.
        MESSAGE e004 INTO sy-msgli.
        result = convert_message( ).
        RETURN.
    ENDTRY.

    result = validator->validate_format_draft_data( tree_data ).
    CHECK result IS INITIAL.

    result = validator->validate_authority_check( activity = cl_ifme_authority_check=>c_auth_actvt_change hierarchytreetype = tree_data->data-version-hierarchytreetype ).
    CHECK result IS INITIAL.

    DATA(tree_builder) = NEW cl_ifme_tree_builder( ).
    DATA(tree) = tree_builder->build( tree_data ).
    DATA(tree_checker) = NEW cl_ifme_tree_checker( tree ).
    result = tree_checker->execute( ).
  ENDMETHOD.


  METHOD constructor.
    IF validator IS INITIAL.
      me->validator = NEW #( ).
    ELSE.
      me->validator = validator.
    ENDIF.

    me->transport_service = cl_ifme_transport_service=>get_instance( ).
  ENDMETHOD.


  METHOD convert_message.
    MESSAGE ID sy-msgid TYPE sy-msgty NUMBER sy-msgno
      WITH sy-msgv1 sy-msgv2 sy-msgv3 sy-msgv4 INTO DATA(msg).
    result = VALUE #( (
      id = sy-msgid
      number = sy-msgno
      message_v1 = sy-msgv1
      message_v2 = sy-msgv2
      message_v3 = sy-msgv3
      message_v4 = sy-msgv4
      type = sy-msgty
      message = msg ) ).
  ENDMETHOD.


  METHOD convert_messages_to_entity.

    ASSIGN COMPONENT 'MESSAGETYPE' OF STRUCTURE entity TO FIELD-SYMBOL(<message_type>).
    ASSIGN COMPONENT 'MESSAGE' OF STRUCTURE entity TO FIELD-SYMBOL(<message_content>).
    CHECK sy-subrc = 0.

    <message_type> = 'S'.
    IF REDUCE char1( INIT ty = space FOR wa IN to_convert WHERE ( type CO 'EAX' ) NEXT ty = wa-type ) IS NOT INITIAL.
      <message_type> = 'E'.
      READ TABLE to_convert ASSIGNING FIELD-SYMBOL(<message>) INDEX 1.
      IF sy-subrc = 0.
        MESSAGE ID <message>-id TYPE <message>-type NUMBER <message>-number
                WITH <message>-message_v1 <message>-message_v2 <message>-message_v3 <message>-message_v4 INTO <message_content>.
      ENDIF.
    ENDIF.

  ENDMETHOD.


  METHOD copy_format_subtable.

    LOOP AT from_table ASSIGNING FIELD-SYMBOL(<current_item>).
      ASSIGN COMPONENT 'ITEMTYPE' OF STRUCTURE <current_item> TO FIELD-SYMBOL(<itemtype>).
      CHECK sy-subrc = 0.

      CASE <itemtype>.
        WHEN cl_ifme_itemtype=>effective.
          "E->E
          APPEND <current_item> TO to_table.

          IF copy_as_child = abap_true.
            "E->P
            APPEND <current_item> TO to_table ASSIGNING FIELD-SYMBOL(<fs>).

            ASSIGN COMPONENT 'ITEMTYPE' OF STRUCTURE <fs> TO FIELD-SYMBOL(<itemtype_to>).
            CHECK sy-subrc = 0.
            <itemtype_to> = cl_ifme_itemtype=>parent.
          ENDIF.

        WHEN cl_ifme_itemtype=>parent.
          IF copy_as_child = abap_false AND copy_as_sibling = abap_true.
            "P->P
            APPEND <current_item> TO to_table.
          ENDIF.

      ENDCASE.

    ENDLOOP.
  ENDMETHOD.


  METHOD copy_format_version_data.
    " 3 situations can happen: copy as child, copy on the same level - top level or sibling level

    target_format = NEW #( ).
    "Format
    target_format->data-format = source_format->data-format.
    IF copy_as_child = abap_true.
      target_format->data-format-hierarchyparenttree = source_format->data-format-hierarchytree.
      target_format->data-format-hierarchytreeredefinitionlevel += 1.
    ELSE.
      DATA(copy_as_sibling) = xsdbool( target_format->data-format-hierarchytreeredefinitionlevel > 0 ).
    ENDIF.
    target_format->data-format_type = source_format->data-format_type.
    target_format->data-format_types_t = source_format->data-format_types_t.

    "Version
    target_format->data-version = source_format->data-version.
    IF copy_as_child = abap_true.
      target_format->data-version-parentversiondatetime = target_format->data-version-lastchangedatetime.
    ENDIF.

    "Flat Attributes
    me->copy_format_subtable(
      EXPORTING
        from_table = source_format->data-flat_attributes
        copy_as_child = copy_as_child
        copy_as_sibling = copy_as_sibling
      IMPORTING
        to_table = target_format->data-flat_attributes
    ).

    "Constants
    me->copy_format_subtable(
      EXPORTING
        from_table = source_format->data-constants
        copy_as_child = copy_as_child
        copy_as_sibling = copy_as_sibling
      IMPORTING
        to_table = target_format->data-constants
    ).

    "Enums
    me->copy_format_subtable(
      EXPORTING
        from_table = source_format->data-enums
        copy_as_child = copy_as_child
        copy_as_sibling = copy_as_sibling
      IMPORTING
        to_table = target_format->data-enums
    ).
    me->copy_format_subtable(
      EXPORTING
        from_table = source_format->data-enums_v
        copy_as_child = copy_as_child
        copy_as_sibling = copy_as_sibling
      IMPORTING
        to_table = target_format->data-enums_v
    ).

    "Nodes
    me->copy_format_subtable(
      EXPORTING
        from_table = source_format->data-nodes
        copy_as_child = copy_as_child
        copy_as_sibling = copy_as_sibling
      IMPORTING
        to_table = target_format->data-nodes
    ).
    IF copy_as_child = abap_true.
      LOOP AT target_format->data-nodes ASSIGNING FIELD-SYMBOL(<node>)
        WHERE itemtype = cl_ifme_itemtype=>effective.
        <node>-hierarchynodestatus = cl_ifme_hierarchynodestatus=>inherited.
      ENDLOOP.
    ENDIF.
    "Fields
    me->copy_format_subtable(
      EXPORTING
        from_table = source_format->data-fields
        copy_as_child = copy_as_child
        copy_as_sibling = copy_as_sibling
      IMPORTING
        to_table = target_format->data-fields
    ).

    "Field Groups
    me->copy_format_subtable(
      EXPORTING
        from_table = source_format->data-field_groups
        copy_as_child = copy_as_child
        copy_as_sibling = copy_as_sibling
      IMPORTING
        to_table = target_format->data-field_groups
    ).

    "Record
    me->copy_format_subtable(
      EXPORTING
        from_table = source_format->data-records
        copy_as_child = copy_as_child
        copy_as_sibling = copy_as_sibling
      IMPORTING
        to_table = target_format->data-records
    ).

    "Record Groups
    me->copy_format_subtable(
      EXPORTING
        from_table = source_format->data-record_groups
        copy_as_child = copy_as_child
        copy_as_sibling = copy_as_sibling
      IMPORTING
        to_table = target_format->data-record_groups
    ).
  ENDMETHOD.


  METHOD copy_mapping_format.
    DATA target_format_key TYPE ifme_pk_fmtvers.

    result = validator->validate_authority_check( activity = cl_ifme_authority_check=>c_auth_actvt_change hierarchytreetype = sourcehierarchytreetype ).
    CHECK result IS INITIAL.

    "Read the ACTIVE version of source format
    DATA(source_format) = cl_ifme_db_service_factory=>get_instance( )->get_db_reader( sourcehierarchytree )->get_format(
      VALUE #(
        hierarchytree = sourcehierarchytree
        hierarchytreetype = sourcehierarchytreetype
        hierarchytreeversion = cl_ifme_hierarchytreeversion=>active
      ) ).

    result = validator->validate_exists_active_version( source_format ).
    CHECK result IS INITIAL.

    result = validator->validate_name_filled( hierarchytree ).
    CHECK result IS INITIAL.

    result = validator->validate_customer_cloud_prefix( hierarchytree ).
    CHECK result IS INITIAL.

    result = validator->validate_can_be_copied(
      fromhierarchytreeparent = source_format->data-format-hierarchyparenttree
      fromhierarchytree = sourcehierarchytree
      tohierarchytree = hierarchytree
      copy_as_child = copy_as_child
    ).
    CHECK result IS INITIAL.

    "Prepare target format MAINTENANCE version key
    target_format_key-hierarchytree = hierarchytree.
    target_format_key-hierarchytreetype = source_format->data-format-hierarchytreetype.
    target_format_key-hierarchytreeversion = cl_ifme_hierarchytreeversion=>maint.

    "Copy data - ACTIVE to MAINT
    DATA(target_format) = copy_format_version_data(
        source_format = source_format
        copy_as_child = copy_as_child
      ).

    "Update target format data keys with new values - ACTIVE to MAINTENANCE + new format name
    cl_ifme_service_common=>update_format_key( new_format_key = target_format_key
                                               mapping_format = target_format ).

    "Update format + version table time stamp and user
    update_timestamp( target_format ).

    TRY.
        cl_ifme_db_service_factory=>get_instance( )->get_db_writer( hierarchytree )->create_format( target_format ).
      CATCH cx_ifme_db.
        MESSAGE e002 WITH hierarchytree INTO sy-msgli.
        result = convert_message( ).
        RETURN.
    ENDTRY.

    " USAGE MEASUREMENT
    CASE sourcehierarchytreetype.
      WHEN cl_ifme_hierarchytreetype=>payment.
        cl_ifme_usage_measurement=>insert_measurement( cl_ifme_usage_measurement=>paym_copy_mapping ).

      WHEN cl_ifme_hierarchytreetype=>treasury.
        cl_ifme_usage_measurement=>insert_measurement( cl_ifme_usage_measurement=>trmc_copy_mapping ).
    ENDCASE.
  ENDMETHOD.


  METHOD create_mapping_format.

    result = validator->validate_authority_check( activity = cl_ifme_authority_check=>c_auth_actvt_change hierarchytreetype = hierarchytreetype ).
    CHECK result IS INITIAL.

    result = validator->validate_name_filled( hierarchytree ).
    CHECK result IS INITIAL.

    result = validator->validate_customer_cloud_prefix( hierarchytree ).
    CHECK result IS INITIAL.

    DATA(format_data) = me->initialize_mapping_format( hierarchytree = hierarchytree
                                                       hierarchytreetype = hierarchytreetype
                                                       description = description
                                                       file_format = file_format ).

    TRY.
        cl_ifme_db_service_factory=>get_instance( )->get_db_writer( hierarchytree )->create_format( format_data ).
      CATCH cx_ifme_db.
        MESSAGE e002 WITH hierarchytree INTO sy-msgli.
        result = convert_message( ).
        RETURN.
    ENDTRY.

    " USAGE MEASUREMENT
    CASE hierarchytreetype.
      WHEN cl_ifme_hierarchytreetype=>payment.
        cl_ifme_usage_measurement=>insert_measurement( cl_ifme_usage_measurement=>paym_create_mapping ).

      WHEN cl_ifme_hierarchytreetype=>treasury.
        cl_ifme_usage_measurement=>insert_measurement( cl_ifme_usage_measurement=>trmc_create_mapping ).
    ENDCASE.
  ENDMETHOD.


  METHOD delete_mapping_format.
    DATA: draft_proc_manager TYPE REF TO if_inc_format_draft_proc.

    "get format and all its versions
    DATA(format_versions) = cl_ifme_db_service_factory=>get_instance( )->get_db_reader( hierarchytree )->get_format_versions( VALUE #( hierarchytree = hierarchytree hierarchytreetype = hierarchytreetype ) ).

    result = validator->validate_authority_check( activity = cl_ifme_authority_check=>c_auth_actvt_change hierarchytreetype = hierarchytreetype ).
    CHECK result IS INITIAL.

    result = validator->validate_can_be_deleted( hierarchytree ).
    CHECK result IS INITIAL.

    result = validator->validate_format_has_children( hierarchytree ).
    CHECK result IS INITIAL.

    result = validator->validate_format_exists( hierarchytree = hierarchytree hierarchytreetype = hierarchytreetype  ).
    CHECK result IS INITIAL.

    TRY.
        IF NEW cl_ifme_common( )->is_customer_format( hierarchytree ).
          draft_proc_manager = cl_inc_format_draft_proc_c=>get_instance( ).
        ELSE.
          draft_proc_manager = cl_inc_format_draft_proc_e=>get_instance( ).
        ENDIF.
      CATCH /bobf/cx_frw.
        MESSAGE e021 WITH hierarchytree INTO sy-msgli.
        result = convert_message( ).
        RETURN.
    ENDTRY.

    "convert version keys
    DATA(versions_uuid_list) = draft_proc_manager->convert_alternative_keys( format_versions ).
    "execute delete - changes are not committed yet
    draft_proc_manager->delete( EXPORTING version_instance_keys = versions_uuid_list
                                IMPORTING eo_message            = DATA(out_message) ).

    " if all went well, commit the changes, otherwise return errors
    IF out_message IS NOT INITIAL.
      result = cl_inc_format_bo_proc_utils=>convert_msg_to_bapiret( out_message ).
      RETURN.
    ENDIF.

    "this validation has to come after modify() that will detect locked draft
    IF deletedraft = abap_false.
      result = validator->validate_exists_draft(
        hierarchytree = hierarchytree hierarchytreetype = hierarchytreetype
        draft_proc = draft_proc_manager
      ).
      IF result IS NOT INITIAL.
        existsunlockeddraft = abap_true.
        RETURN.
      ENDIF.

    ENDIF.

    cl_inc_format_bo_proc_utils=>get_transaction_manager( )->save( ).
    cl_ifme_db_service_factory=>get_instance( )->get_db_writer( hierarchytree )->delete_format( VALUE #( hierarchytree = hierarchytree hierarchytreetype = hierarchytreetype ) ).

    TRY.
        "Write object to the transport request
        save_transport( format_key = VALUE #( hierarchytree = hierarchytree hierarchytreetype = hierarchytreetype ) request = request ).
      CATCH cx_ifme.
        result = convert_message( ).
        ROLLBACK WORK.
    ENDTRY.
  ENDMETHOD.


  METHOD export_mapping_format.

    result = validator->validate_authority_check( activity = cl_ifme_authority_check=>c_auth_actvt_display hierarchytreetype = hierarchytreetype ).
    CHECK result IS INITIAL.

    TRY.
        DATA(tree_data) = NEW cl_ifme_tree_builder( )->build_tree( VALUE #( hierarchytree = hierarchytree
                                                                            hierarchytreetype = hierarchytreetype
                                                                            hierarchytreeversion = cl_ifme_hierarchytreeversion=>active ) ).
      CATCH cx_ifme.
        result = convert_message( ).
        RETURN.
    ENDTRY.

    DATA(downloader) = NEW cl_ifme_xml_download( tree_data ).

    TRY.
        downloader->execute( ).
      CATCH cx_ifme_xml.
        result = convert_message( ).
        RETURN.
    ENDTRY.

    xml_xstring = downloader->convert_solix_to_xstring( ).

    " USAGE MEASUREMENT
    CASE hierarchytreetype.
      WHEN cl_ifme_hierarchytreetype=>payment.
        cl_ifme_usage_measurement=>insert_measurement( cl_ifme_usage_measurement=>paym_export_xml ).

      WHEN cl_ifme_hierarchytreetype=>treasury.
        cl_ifme_usage_measurement=>insert_measurement( cl_ifme_usage_measurement=>trmc_export_xml ).
    ENDCASE.
  ENDMETHOD.


  METHOD get_backup_version_number.

    DATA version_numc TYPE numc06.

    DATA(db_service) = cl_ifme_db_service_factory=>get_instance( )->get_db_reader( format_key-hierarchytree ).

    DATA(versions) = db_service->get_format_versions( format_key ).

    DELETE versions WHERE hierarchytreeversion EQ cl_ifme_hierarchytreeversion=>maint
                       OR hierarchytreeversion EQ cl_ifme_hierarchytreeversion=>active.

    IF versions IS INITIAL.
      version_numc = 1.
    ELSE.
      SORT versions DESCENDING.
      version_numc = versions[ 1 ]-hierarchytreeversion + 1.
    ENDIF.

    version_number = version_numc.

  ENDMETHOD.


  METHOD get_draft_format_data.
    DATA: draft_reader TYPE REF TO if_ifme_service_draft_reader.

    DATA(draft_processor) = get_draft_processor( is_customer ).

    IF is_customer = abap_true.
      draft_reader = CAST cl_inc_format_draft_proc_c( draft_processor )->get_reader( ).
    ELSE.
      draft_reader = CAST cl_inc_format_draft_proc_e( draft_processor )->get_reader( ).
    ENDIF.

    result = draft_reader->get_format_data( draft_root_instance_key ).
  ENDMETHOD.


  METHOD get_draft_processor.
    IF is_customer = abap_true.
      result = cl_inc_format_draft_proc_c=>get_instance( ).
    ELSE.
      result = cl_inc_format_draft_proc_e=>get_instance( ).
    ENDIF.
  ENDMETHOD.


  METHOD get_format_structure.

    TEST-SEAM get_format_structure.
      DATA(ls_ifmefmttype) = cl_ifmefmttype=>get_instance( )->get_single( iv_hierarchytreetype ).
    END-TEST-SEAM.

    cl_ifme_struct_field_provider=>get(
      EXPORTING iv_structurename = ls_ifmefmttype-outputstructurename
      IMPORTING et_fields        = DATA(lt_fields) ).

    MOVE-CORRESPONDING lt_fields TO et_fields.

  ENDMETHOD.


  METHOD get_transport_request.
    request_list = me->transport_service->get_request( CONV #( hierarchytreetype && hierarchytree ) ).
  ENDMETHOD.


  METHOD create_maintenance.
    result = validator->validate_authority_check( activity          = cl_ifme_authority_check=>c_auth_actvt_change
                                                  hierarchytreetype = hierarchytreetype ).
    CHECK result IS INITIAL.

    " check existence of MAINT version in DB
    DATA(db_key) = VALUE ifme_pk_fmtvers( hierarchytreeversion = cl_ifme_hierarchytreeversion=>maint
                                          hierarchytreetype    = hierarchytreetype
                                          hierarchytree        = hierarchytree ).
    IF cl_ifme_db_service_factory=>get_instance( )->get_db_reader( db_key-hierarchytree )->exists_format_version( db_key ) = abap_false.
      "read ACTIVE
      db_key-hierarchytreeversion = cl_ifme_hierarchytreeversion=>active.
      DATA(format_data) = cl_ifme_db_service_factory=>get_instance( )->get_db_reader( db_key-hierarchytree )->get_format( db_key ).

      "change to maint
      db_key-hierarchytreeversion = cl_ifme_hierarchytreeversion=>maint.
      cl_ifme_service_common=>update_format_key( new_format_key = db_key
                                                 mapping_format = format_data ).
      "update administrative data of MAINT version
      CLEAR format_data->data-version-lastchangedatetime.
      CLEAR format_data->data-version-lastchangebyuser.
      GET TIME STAMP FIELD DATA(ts).
      format_data->data-format-creationdatetime = ts.
      format_data->data-format-createdbyuser    = sy-uname.

      TRY.
          cl_ifme_db_service_factory=>get_instance( )->get_db_writer( db_key-hierarchytree )->create_version( format_data ).
        CATCH cx_ifme_db.
          MESSAGE e026 INTO sy-msgli.
          result = convert_message( ).
          RETURN.
      ENDTRY.
    ENDIF.
  ENDMETHOD.


  METHOD import_mapping_format.

    DATA draft_proc_manager TYPE REF TO if_inc_format_draft_proc.

    result = validator->validate_authority_check( activity = cl_ifme_authority_check=>c_auth_actvt_change hierarchytreetype = hierarchytreetype ).
    CHECK result IS INITIAL.

    TRY.
        IF NEW cl_ifme_common( )->is_customer_format( hierarchytree ).
          draft_proc_manager = cl_inc_format_draft_proc_c=>get_instance( ).
        ELSE.
          draft_proc_manager = cl_inc_format_draft_proc_e=>get_instance( ).
        ENDIF.
      CATCH /bobf/cx_frw.
        MESSAGE e021 WITH hierarchytree INTO DATA(msg).
        result = convert_message( ).
        RETURN.
    ENDTRY.

    DATA(result_draft) = validator->validate_exists_draft( hierarchytree = hierarchytree hierarchytreetype = hierarchytreetype draft_proc = draft_proc_manager ).

    IF result_draft IS NOT INITIAL.

      DATA(format_versions) = cl_ifme_db_service_factory=>get_instance( )->get_db_reader( hierarchytree )->get_format_versions( VALUE #( hierarchytree = hierarchytree hierarchytreetype = hierarchytreetype ) ).
      DELETE format_versions WHERE hierarchytreeversion <> cl_ifme_hierarchytreeversion=>maint.
      DATA(versions_uuid_list) = draft_proc_manager->convert_alternative_keys( format_versions ).

      " check if the draft is locked and try to edit it
      draft_proc_manager->edit( EXPORTING root_instance_key = versions_uuid_list[ 1 ]-key
                                IMPORTING eo_message = DATA(out_message)
                                          et_failed_key = DATA(out_failed_keys) ).

      IF out_failed_keys IS NOT INITIAL OR
         delete_draft = abap_false.
        cl_inc_format_bo_proc_utils=>get_transaction_manager( )->cleanup( ).
      ENDIF.

      IF out_failed_keys IS NOT INITIAL.
        result = cl_inc_format_bo_proc_utils=>convert_msg_to_bapiret( out_message ).
        RETURN.
      ENDIF.

    ENDIF.

    " try to parse XML file
    DATA(xml_upload) = NEW cl_ifme_xml_upload( i_tree_key = VALUE #( hierarchytreetype    = hierarchytreetype
                                                                     hierarchytree        = hierarchytree
                                                                     hierarchytreeversion = cl_ifme_hierarchytreeversion=>maint )
                                               i_xml_data = cl_bcs_convert=>xstring_to_solix( xml_xstring ) ).
    TRY.
        xml_upload->execute( ).
      CATCH cx_ifme_xml.
        result = convert_message( ).
        RETURN.
    ENDTRY.

    "this validation has to come after edit() that will detect locked draft
    IF delete_draft = abap_false AND
       result_draft IS NOT INITIAL.
      result = result_draft.
      exists_unlocked_draft = abap_true.
      RETURN.
    ENDIF.

    "backup active version
    TRY.
        DATA(active_version) = cl_ifme_db_service_factory=>get_instance( )->get_db_reader( hierarchytree )->get_format(
          VALUE #( hierarchytreetype    = hierarchytreetype
                   hierarchytree        = hierarchytree
                   hierarchytreeversion = cl_ifme_hierarchytreeversion=>active ) ).
        IF active_version->data-version-hierarchytreeversion IS NOT INITIAL.
          backup_version( active_version ).
        ENDIF.
      CATCH cx_ifme_service.
        MESSAGE e018 INTO msg.
        result = convert_message( ).
        RETURN.
    ENDTRY.

    " import XML file
    TRY.
        DATA(db_service) = cl_ifme_db_service_factory=>get_instance( )->get_db_writer( hierarchytree ).
      db_service->delete_version( VALUE ifme_pk_fmtvers( hierarchytree = hierarchytree hierarchytreetype = hierarchytreetype hierarchytreeversion = cl_ifme_hierarchytreeversion=>maint ) ).
        db_service->create_version( xml_upload->get_tree_data( ) ).
      CATCH cx_ifme_db.
        MESSAGE e013 INTO msg.
        result = convert_message( ).
        RETURN.
    ENDTRY.

    " delete draft
    IF result_draft IS NOT INITIAL.
      cl_inc_format_bo_proc_utils=>get_transaction_manager( )->save( ).
      result = delete_draft( draft_uuid = cl_ifme_db_service_factory=>get_instance( )->get_db_reader( hierarchytree )->get_draft_uuid(
                                            VALUE #( hierarchytreetype    = hierarchytreetype
                                                     hierarchytree        = hierarchytree
                                                     hierarchytreeversion = cl_ifme_hierarchytreeversion=>maint ) )
                             draft_proc_manager = draft_proc_manager ).
      CHECK result IS INITIAL.
    ENDIF.

    " USAGE MEASUREMENT
    CASE hierarchytreetype.
      WHEN cl_ifme_hierarchytreetype=>payment.
        cl_ifme_usage_measurement=>insert_measurement( cl_ifme_usage_measurement=>paym_import_xml ).

      WHEN cl_ifme_hierarchytreetype=>treasury.
        cl_ifme_usage_measurement=>insert_measurement( cl_ifme_usage_measurement=>trmc_import_xml ).
    ENDCASE.

  ENDMETHOD.


  METHOD initialize_mapping_format.
    DATA:
      format_key TYPE ifme_pk_fmtvers,
      root_node  TYPE ifmenode.

    format_key-hierarchytree = hierarchytree.
    format_key-hierarchytreetype = hierarchytreetype.
    format_key-hierarchytreeversion = cl_ifme_hierarchytreeversion=>maint.

    GET TIME STAMP FIELD DATA(ts).

    format_data = NEW #( ).
    format_data->data-format = VALUE #(
      hierarchytree = format_key-hierarchytree
      hierarchytreetype = format_key-hierarchytreetype
      creationdatetime = ts
      createdbyuser = sy-uname
      inputfileformat = file_format
    ).

    format_data->data-version = VALUE #(
      hierarchytree = format_key-hierarchytree
      hierarchytreetype = format_key-hierarchytreetype
      hierarchytreeversion = format_key-hierarchytreeversion
      hierarchytreeversionshorttext = description
      lastchangedatetime = ts
      lastchangebyuser = sy-uname
    ).

    root_node = VALUE #(
      hierarchytree = format_key-hierarchytree
      hierarchytreetype = format_key-hierarchytreetype
      hierarchytreeversion = format_key-hierarchytreeversion
      hierarchynode = cl_ifme_referenceid=>root_node_id
      itemtype = cl_ifme_itemtype=>effective
      nodetype = cl_ifme_nodetype=>root
      nodename = TEXT-001
*      hierarchynodeshorttext = TEXT-001
      hierarchynodestatus = cl_ifme_hierarchynodestatus=>current
    ).

    format_data->data-nodes = VALUE #( ( root_node ) ).

    CASE to_upper( file_format ).
      WHEN cl_ifme_inputfileformat=>flat.
        DATA: flat_attribute TYPE ifmefmtattrf.

        flat_attribute-hierarchytree = format_key-hierarchytree.
        flat_attribute-hierarchytreetype = format_key-hierarchytreetype.
        flat_attribute-hierarchytreeversion = format_key-hierarchytreeversion.
        flat_attribute-itemtype = cl_ifme_itemtype=>effective.
        flat_attribute-fielddelimitertype = cl_ifme_fmtfielddelimitertype=>no_delimiter.
        flat_attribute-decimalseparatorcharactervalue = cl_ifme_fmtdecimalseparvalue=>none.
        format_data->data-flat_attributes = VALUE #( ( flat_attribute ) ).

      WHEN cl_ifme_inputfileformat=>xml.
        "TODO: implement when table created
    ENDCASE.
  ENDMETHOD.


  METHOD save_transport.
    me->transport_service->write_to_request( format_key = format_key request = request ).
  ENDMETHOD.


  METHOD update_timestamp.
    GET TIME STAMP FIELD DATA(ts).
    mapping_format->data-format-creationdatetime = ts.
    mapping_format->data-format-createdbyuser = sy-uname.
    mapping_format->data-version-lastchangedatetime = ts.
    mapping_format->data-version-lastchangebyuser = sy-uname.
  ENDMETHOD.


  METHOD delete_draft.
    draft_proc_manager->delete( EXPORTING version_instance_keys = VALUE #( ( key = draft_uuid ) )
                                IMPORTING eo_message = DATA(out_message) ).

    IF out_message IS NOT INITIAL.
      out_message->get_messages( EXPORTING iv_severity = /bobf/cm_frw=>co_severity_error
                                 IMPORTING et_message = DATA(error_messages) ).
      IF error_messages IS NOT INITIAL.
        result = cl_inc_format_bo_proc_utils=>convert_msg_to_bapiret( out_message ).
        cl_inc_format_bo_proc_utils=>get_transaction_manager( )->cleanup( ).
        RETURN.
      ENDIF.
    ENDIF.
    cl_inc_format_bo_proc_utils=>get_transaction_manager( )->save( ).
  ENDMETHOD.
ENDCLASS.
