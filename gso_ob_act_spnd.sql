SELECT 
    spnd."event_id_merc__c",
    spnd."event_country_merc__c",
    spnd."date_of_event_merc__c",
    spnd."owner_master_id_merc__c",
    spnd."name",
    att."event_id_merc__c" as event_id_merc__c1,
    att.id as id1,
    att."customer_id_glbl__c",
    att."name_glbl__c",
    att."record_type_name__c",
    att."currencyisocode", 
    att."total_hotel_tov_merc__c",
    att."total_ground_trnsprtn_tov_merc",
    att."total_food_beverage_tov_merc",
    att."total_registration_tov_merc__c",
    att."total_indiv_trans_tov_merc__c",
    att."copay_hotel_merc__c",
    att."copay_ground_transport_merc__c",
    att."copay_food_beverage_merc__c",
    att."copay_registration_merc__c",
    att."copay_flight_rail_merc__c",
    att."copay_aods_indiv_trans_merc__c",
    tov.id,
    tov."meeting_participant_merc__c" as meeting_participant_merc__c1,
    tov."meeting_day_date_merc__c",
    tov."currencyisocode" as currencyisocode_tov,
    tov."est_hotel_tov_merc__c",
    tov."est_grnd_transp_merc__c",
    tov."est_food_bev_tov_merc__c",
    tov."est_reg_amt_tov_merc__c",
    tov."est_indv_transfer_merc__c"
FROM 
    gs_edb_cms.GSO_OB_ACT_SPND_STG AS spnd
JOIN 
    gs_edb_cms.GSO_OB_ACT_SPND_ATT_STG AS att 
    ON spnd."event_id_merc__c" = att."event_id_merc__c"
LEFT JOIN 
    gs_edb_cms.GSO_OB_ACT_SPND_ATT_TOV_STG AS tov 
    ON att."meeting_participant_id_merc__c" = tov."meeting_participant_merc__c";