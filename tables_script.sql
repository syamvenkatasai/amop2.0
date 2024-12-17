CREATE TABLE IF NOT EXISTS public.integration
(
    id serial primary key,
    name character varying(50) ,
    website character varying(250) ,
    created_by character varying(100) ,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) ,
    deleted_date timestamp without time zone,
    modified_by character varying(100) ,
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    is_active boolean,
    authentication_type integer,
    has_service_provider boolean,
    portal_type_id integer
);
CREATE INDEX IF NOT EXISTS idx_i_id
    ON public.integration USING btree
    (id ASC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_i_is_active
    ON public.integration USING btree
    (is_active ASC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_integration_portal_type_id
    ON public.integration USING btree
    (portal_type_id ASC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_integration_portal_type_id_active_deleted
    ON public.integration USING btree
    (portal_type_id ASC NULLS LAST, is_active ASC NULLS LAST);

CREATE TABLE IF NOT EXISTS public.serviceprovider
(
    id serial primary key,
    service_provider_name character varying(50)  DEFAULT NULL::character varying,
    display_name character varying(50) ,
    created_by character varying(100)  DEFAULT NULL::character varying,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) ,
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) ,
    deleted_date timestamp without time zone,
    is_active boolean,
    integration_id integer,
    bill_period_end_day integer,
    bill_period_end_hour integer,
    device_detail_service_provider_id integer,
    tenant_id integer,
    optimization_start_hour_local_time integer,
    continuous_last_day_optimization_start_hour_local_time integer,
    write_is_enabled boolean,
    register_carrier_service_callback boolean,
    opt_into_carrier_optimization boolean,
    CONSTRAINT unique_id_serviceprovider UNIQUE (id, service_provider_name),
    CONSTRAINT fk_integration_id FOREIGN KEY (integration_id)
        REFERENCES public.integration (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_serviceprovider_is_active
    ON public.serviceprovider USING btree
    (is_active ASC NULLS LAST)
    WHERE is_active = true;

CREATE INDEX IF NOT EXISTS idx_serviceprovider_tenant_id
    ON public.serviceprovider USING btree
    (tenant_id ASC NULLS LAST);

CREATE INDEX IF NOT EXISTS idx_sp_id
    ON public.serviceprovider USING btree
    (id ASC NULLS LAST);

CREATE INDEX IF NOT EXISTS idx_sp_integration_id
    ON public.serviceprovider USING btree
    (integration_id ASC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_sp_is_active
    ON public.serviceprovider USING btree
    (is_active ASC NULLS LAST);

CREATE TABLE IF NOT EXISTS public.bandwidthaccount
(
    id serial primary key,
    service_provider_id integer,
    account_id character varying(50)  DEFAULT NULL::character varying,
    global_account_number character varying(50)  DEFAULT NULL::character varying,
    associated_catapult_account character varying(50)  DEFAULT NULL::character varying,
    company_name character varying(50)  DEFAULT NULL::character varying,
    account_type character varying(50)  DEFAULT NULL::character varying,
    billing_cycle integer,
    created_by character varying(128)  DEFAULT NULL::character varying,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(128)  DEFAULT NULL::character varying,
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(128)  DEFAULT NULL::character varying,
    deleted_date timestamp without time zone,
    is_active boolean,
    tenant_id integer,
    tenant_name character varying(80),
    CONSTRAINT fk_service_provider_id FOREIGN KEY (service_provider_id)
        REFERENCES public.serviceprovider (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS public.bandwidth_customers
(
    id serial primary key,
    bandwidth_account_id integer,
    customer_id integer,
    bandwidth_customer_name character varying(50) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    sip_peer_count integer,
    created_by character varying(128) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(128) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(128) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    deleted_date timestamp without time zone,
    is_active boolean,
    CONSTRAINT fk_bandwidth_account_id FOREIGN KEY (bandwidth_account_id)
        REFERENCES public.bandwidthaccount (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL
);
-- Index: idx_bandwidth_site_bandwidth_account_id

-- DROP INDEX IF EXISTS public.idx_bandwidth_site_bandwidth_account_id;

CREATE INDEX IF NOT EXISTS idx_bandwidth_site_bandwidth_account_id
    ON public.bandwidth_customers USING btree
    (bandwidth_account_id ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS public.billing_period_status
(
    id serial primary key,
    display_name character varying(25) COLLATE pg_catalog."default" NOT NULL,
    display_order integer NOT NULL,
    created_by character varying(100) COLLATE pg_catalog."default" NOT NULL DEFAULT 'System'::character varying,
    created_date timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean NOT NULL DEFAULT true
);

CREATE TABLE IF NOT EXISTS public.billing_period
(
    id serial primary key,
    service_provider_id integer,
    service_provider character varying(100) COLLATE pg_catalog."default",
    bill_year integer,
    bill_month integer,
    billing_cycle_start_date timestamp without time zone,
    billing_cycle_end_date timestamp without time zone,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean,
    billing_period_status_id integer,
    tenant_id integer,
    CONSTRAINT fk_billing_period_status_id FOREIGN KEY (billing_period_status_id)
        REFERENCES public.billing_period_status (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL,
    CONSTRAINT fk_service_provider_id FOREIGN KEY (service_provider_id)
        REFERENCES public.serviceprovider (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL
);
-- Index: idx_billing_cycle_end_date

-- DROP INDEX IF EXISTS public.idx_billing_cycle_end_date;

CREATE INDEX IF NOT EXISTS idx_billing_cycle_end_date
    ON public.billing_period USING btree
    (billing_cycle_end_date ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_billing_period_id

-- DROP INDEX IF EXISTS public.idx_billing_period_id;

CREATE INDEX IF NOT EXISTS idx_billing_period_id
    ON public.billing_period USING btree
    (id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_billing_period_service_provider_id

-- DROP INDEX IF EXISTS public.idx_billing_period_service_provider_id;

CREATE INDEX IF NOT EXISTS idx_billing_period_service_provider_id
    ON public.billing_period USING btree
    (service_provider_id ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS public.bulk_change_popup_screens
(
    id serial primary key,
    service_provider_id integer,
    service_provider character varying(200) COLLATE pg_catalog."default",
    change_type_id integer,
    change_type character varying(255) COLLATE pg_catalog."default",
    screen_names_seq text COLLATE pg_catalog."default",
    dropdown_fields text COLLATE pg_catalog."default"
);

CREATE TABLE IF NOT EXISTS public.carrier_rate_plan
(
    id serial primary key,
    rate_plan_code character varying(255) COLLATE pg_catalog."default",
    base_rate numeric(25,10),
    plan_mb numeric(25,10),
    overage_rate_cost numeric(25,10),
    surcharge_3g numeric(25,10),
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    rate_charge_amt numeric(25,10),
    display_rate numeric(26,10),
    base_rate_per_mb numeric(38,12),
    is_active boolean,
    rate_plan_short_name character varying(255) COLLATE pg_catalog."default",
    jasper_rate_plan_id bigint,
    service_provider_id integer,
    data_per_overage_charge numeric(25,10),
    allows_sim_pooling boolean,
    friendly_name character varying(256) COLLATE pg_catalog."default",
    is_retired boolean,
    amount_with_deal_registration numeric(25,10),
    amount_without_deal_registration numeric(25,10),
    family character varying(100) COLLATE pg_catalog."default",
    device_type character varying(250) COLLATE pg_catalog."default",
    imei_type character varying(250) COLLATE pg_catalog."default",
    os character varying(50) COLLATE pg_catalog."default",
    network character varying(50) COLLATE pg_catalog."default",
    description character varying(500) COLLATE pg_catalog."default",
    plan_uuid character varying(50) COLLATE pg_catalog."default",
    is_exclude_from_optimization boolean,
    optimization_rate_plan_type_id integer,
    default_optimization_group_id integer,
    service_provider character varying(80) COLLATE pg_catalog."default",
    CONSTRAINT unique_id_rate_plan_name UNIQUE (id, rate_plan_code),
    CONSTRAINT fk_service_provider_id FOREIGN KEY (service_provider_id)
        REFERENCES public.serviceprovider (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_carrier_rate_plan_id
    ON public.carrier_rate_plan USING btree
    (id ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS public.customer_rate_pool
(
    id serial primary key,
    name character varying(200) COLLATE pg_catalog."default",
    service_provider_id integer,
    tenant_id integer,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    is_active boolean,
    service_provider_ids text COLLATE pg_catalog."default",
    service_provider_name character varying(80) COLLATE pg_catalog."default",
    projected_usage character varying COLLATE pg_catalog."default",
    CONSTRAINT fk_service_provider_id FOREIGN KEY (service_provider_id)
        REFERENCES public.serviceprovider (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL
);
-- Index: idx_customer_rate_pool_id

-- DROP INDEX IF EXISTS public.idx_customer_rate_pool_id;

CREATE INDEX IF NOT EXISTS idx_customer_rate_pool_id
    ON public.customer_rate_pool USING btree
    (id ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS public.customerrateplan
(
    id serial primary key,
    rate_plan_code character varying(50) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    plan_mb numeric(25,10) NOT NULL,
    base_rate numeric(25,10),
    surcharge_3g numeric(25,10),
    min_plan_data_mb numeric(25,10),
    max_plan_data_mb numeric(25,10),
    created_by character varying(100) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    rate_plan_name character varying(100) COLLATE pg_catalog."default",
    rate_charge_amt numeric(25,10) NOT NULL,
    display_rate numeric(26,10),
    base_rate_per_mb numeric(38,12),
    is_active boolean,
    service_provider_id integer,
    data_per_overage_charge numeric(25,10) NOT NULL,
    overage_rate_cost numeric(25,10) NOT NULL,
    allows_sim_pooling boolean,
    tenant_id integer,
    is_billing_advance_eligible boolean,
    sms_rate numeric(25,10) NOT NULL,
    auto_change_rate_plan boolean,
    serviceproviderids text COLLATE pg_catalog."default",
    service_provider_name character varying(80) COLLATE pg_catalog."default",
    automation_rule character varying(255) COLLATE pg_catalog."default",
    optimization_type character varying COLLATE pg_catalog."default",
    active_inactive_status character varying COLLATE pg_catalog."default",
    CONSTRAINT fk_service_provider_id FOREIGN KEY (service_provider_id)
        REFERENCES public.serviceprovider (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_customerrateplan_id
    ON public.customerrateplan USING btree
    (id ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS public.netsapiens_reseller
(
    id serial primary key,
    territory_id integer,
    territory character varying(256) COLLATE pg_catalog."default",
    description character varying(1024) COLLATE pg_catalog."default",
    entry_status character varying(50) COLLATE pg_catalog."default",
    smtp_host character varying(512) COLLATE pg_catalog."default",
    smtp_port character varying(50) COLLATE pg_catalog."default",
    smtp_uid character varying(256) COLLATE pg_catalog."default",
    smtp_pwd character varying(256) COLLATE pg_catalog."default",
    users integer,
    domains integer,
    count_for_limit integer,
    count_external integer,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean NOT NULL,
    can_ignore boolean NOT NULL,
    tenant_id integer
);

CREATE TABLE IF NOT EXISTS public.e911customers
(
    id serial primary key,
    account_name character varying(256) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    account_id character varying(255) COLLATE pg_catalog."default",
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    deleted_date timestamp without time zone,
    is_active boolean
);

CREATE TABLE service_provider_setting (
    id SERIAL PRIMARY KEY,
    setting_key VARCHAR(50) NOT NULL,
    setting_value TEXT NOT NULL,
    data_type_id INT NOT NULL,
    created_by VARCHAR(100) NOT NULL,
    created_date TIMESTAMP NOT NULL,
    modified_by VARCHAR(100),
    modified_date TIMESTAMP,
    deleted_by VARCHAR(100),
    deleted_date TIMESTAMP,
    service_provider_id INT,
    is_active BOOLEAN NOT NULL,
    CONSTRAINT fk_service_provider_setting_service_provider
        FOREIGN KEY (service_provider_id) REFERENCES serviceprovider(id)
);

CREATE TABLE IF NOT EXISTS public.integration_authentication
(
    id serial primary key,
    integration_id integer,
    authentication_type integer,
    username character varying(255) COLLATE pg_catalog."default",
    password character varying(255) COLLATE pg_catalog."default",
    oauth2_authorization_url character varying(255) COLLATE pg_catalog."default",
    oauth2_token_url character varying(255) COLLATE pg_catalog."default",
    oauth2_refresh_url character varying(255) COLLATE pg_catalog."default",
    oauth2_authorization_code character varying(255) COLLATE pg_catalog."default",
    oauth2_authorization_header character varying(255) COLLATE pg_catalog."default",
    oauth2_state character varying(255) COLLATE pg_catalog."default",
    oauth2_scope character varying(255) COLLATE pg_catalog."default",
    oauth2_client_id character varying(255) COLLATE pg_catalog."default",
    oauth2_client_secret character varying(255) COLLATE pg_catalog."default",
    oauth2_access_token text COLLATE pg_catalog."default",
    oauth2_refresh_token character varying(1000) COLLATE pg_catalog."default",
    oauth2_access_token_expires bigint,
    oauth2_refresh_token_expires bigint,
    oauth2_custom1 character varying(255) COLLATE pg_catalog."default",
    oauth2_custom2 character varying(255) COLLATE pg_catalog."default",
    oauth2_custom3 character varying(255) COLLATE pg_catalog."default",
    oauth2_custom4 character varying(255) COLLATE pg_catalog."default",
    token_location character varying(10) COLLATE pg_catalog."default",
    token_variable_name character varying(255) COLLATE pg_catalog."default",
    token_value character varying(255) COLLATE pg_catalog."default",
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    is_active boolean,
    tenant_id integer,
    realm_id character varying(50) COLLATE pg_catalog."default",
    service_provider_id integer,
    is_child_tenant boolean,
    rev_bill_profile text COLLATE pg_catalog."default",
    CONSTRAINT fk_integration_id FOREIGN KEY (integration_id)
        REFERENCES public.integration (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL,
    CONSTRAINT fk_service_provider_id FOREIGN KEY (service_provider_id)
        REFERENCES public.serviceprovider (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_ia_id
    ON public.integration_authentication USING btree
    (id ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS public.revcustomer
(
    id  uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    rev_customer_id character varying(50) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    customer_name character varying(250) COLLATE pg_catalog."default",
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean,
    rev_parent_customer_id character varying(50) COLLATE pg_catalog."default",
    parent_customer_id uuid,
    child_count integer,
    integration_authentication_id integer,
    status character varying(50) COLLATE pg_catalog."default",
    activated_date timestamp without time zone,
    close_date timestamp without time zone,
    tax_exempt_enabled boolean,
    tax_exempt_types character varying COLLATE pg_catalog."default",
    bill_profile_id integer,
    agent_id integer,
    tenant_id integer,
    CONSTRAINT fk_integration_authentication_id FOREIGN KEY (integration_authentication_id)
        REFERENCES public.integration_authentication (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL,
    CONSTRAINT fk_parent_customer_id FOREIGN KEY (parent_customer_id)
        REFERENCES public.revcustomer (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_r_id
    ON public.revcustomer USING btree
    (id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_r_status_is_active_deleted

-- DROP INDEX IF EXISTS public.idx_r_status_is_active_deleted;

CREATE INDEX IF NOT EXISTS idx_r_status_is_active
    ON public.revcustomer USING btree
    (status COLLATE pg_catalog."default" ASC NULLS LAST, is_active ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_rev_customer_active_deleted

-- DROP INDEX IF EXISTS public.idx_rev_customer_active_deleted;

CREATE INDEX IF NOT EXISTS idx_rev_customer_active
    ON public.revcustomer USING btree
    (rev_customer_id COLLATE pg_catalog."default" ASC NULLS LAST, is_active ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_revcustomer_active

-- DROP INDEX IF EXISTS public.idx_revcustomer_active;

CREATE INDEX IF NOT EXISTS idx_revcustomer_active
    ON public.revcustomer USING btree
    (rev_customer_id COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default
    WHERE status::text <> 'CLOSED'::text AND is_active = true;
-- Index: idx_revcustomer_active_deleted

-- DROP INDEX IF EXISTS public.idx_revcustomer_active_deleted;

CREATE INDEX IF NOT EXISTS idx_revcustomer_active_deleted
    ON public.revcustomer USING btree
    (is_active ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_revcustomer_integration_authentication_id

-- DROP INDEX IF EXISTS public.idx_revcustomer_integration_authentication_id;

CREATE INDEX IF NOT EXISTS idx_revcustomer_integration_authentication_id
    ON public.revcustomer USING btree
    (integration_authentication_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_revcustomer_is_active

-- DROP INDEX IF EXISTS public.idx_revcustomer_is_active;

CREATE INDEX IF NOT EXISTS idx_revcustomer_is_active
    ON public.revcustomer USING btree
    (is_active ASC NULLS LAST)
    TABLESPACE pg_default;


-- Index: idx_revcustomer_rev_customer_id

-- DROP INDEX IF EXISTS public.idx_revcustomer_rev_customer_id;

CREATE INDEX IF NOT EXISTS idx_revcustomer_rev_customer_id
    ON public.revcustomer USING btree
    (rev_customer_id COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_revcustomer_status

-- DROP INDEX IF EXISTS public.idx_revcustomer_status;

CREATE INDEX IF NOT EXISTS idx_revcustomer_status
    ON public.revcustomer USING btree
    (status COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_revcustomer_status_is_active
    ON public.revcustomer USING btree
    (status COLLATE pg_catalog."default" ASC NULLS LAST, is_active ASC NULLS LAST)
    WHERE status::text <> 'CLOSED'::text AND is_active = true;

CREATE TABLE IF NOT EXISTS public.customers
(
    id serial primary key,
    tenant_id integer,
    tenant_name character varying(350) COLLATE pg_catalog."default",
    subtenant_name character varying COLLATE pg_catalog."default",
    customer_id integer,
    customer_name character varying(250) COLLATE pg_catalog."default",
    billing_account_number character varying COLLATE pg_catalog."default",
    customer_rate_plans character varying COLLATE pg_catalog."default",
    customer_bill_period_end_date character varying COLLATE pg_catalog."default",
    customer_bill_period_end_hour integer,
    customer_bill_period_end_day integer,
    apt_suite character varying(20) COLLATE pg_catalog."default",
    address1 character varying(150) COLLATE pg_catalog."default",
    address2 character varying(150) COLLATE pg_catalog."default",
    city character varying(50) COLLATE pg_catalog."default",
    state character varying(50) COLLATE pg_catalog."default",
    postal_code character varying(20) COLLATE pg_catalog."default",
    postal_code_extension character varying(20) COLLATE pg_catalog."default",
    country character varying(100) COLLATE pg_catalog."default",
    rev_customer_id uuid,
    bandwidth_customer_id integer,
    netsapiens_customer_id integer,
    e911_customer_id integer,
    netsapiens_domain_id integer,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    is_active boolean,
    first_name character varying COLLATE pg_catalog."default",
    middle_initial character varying COLLATE pg_catalog."default",
    last_name character varying COLLATE pg_catalog."default",
    company_name character varying COLLATE pg_catalog."default",
    description character varying(1000) COLLATE pg_catalog."default",
    inactivity_start character varying COLLATE pg_catalog."default",
    inactivity_end character varying COLLATE pg_catalog."default",
    county_parish_borough character varying(50) COLLATE pg_catalog."default",
    netsapiens_type character varying(50) COLLATE pg_catalog."default",
    parent_customer_id integer,
    is_system_default boolean,
    status character varying COLLATE pg_catalog."default",
    rev_io_account character varying COLLATE pg_catalog."default",
    bill_profile character varying COLLATE pg_catalog."default",
    parent_customer character varying COLLATE pg_catalog."default",
    service_address boolean,
    service_address_first_name character varying COLLATE pg_catalog."default",
    service_address_middle_initial character varying COLLATE pg_catalog."default",
    service_address_last_name character varying COLLATE pg_catalog."default",
    service_address_company_name character varying COLLATE pg_catalog."default",
    service_address_address_line_1 character varying COLLATE pg_catalog."default",
    service_address_address_line_2 character varying COLLATE pg_catalog."default",
    service_address_city character varying COLLATE pg_catalog."default",
    service_address_state character varying COLLATE pg_catalog."default",
    service_address_postal_code character varying COLLATE pg_catalog."default",
    service_address_postal_code_extension character varying COLLATE pg_catalog."default",
    service_address_country_code character varying COLLATE pg_catalog."default",
    billing_address boolean,
    billing_address_first_name character varying COLLATE pg_catalog."default",
    billing_address_middle_initial character varying COLLATE pg_catalog."default",
    billing_address_last_name character varying COLLATE pg_catalog."default",
    billing_address_company_name character varying COLLATE pg_catalog."default",
    billing_address_address_line_1 character varying COLLATE pg_catalog."default",
    billing_address_address_line_2 character varying COLLATE pg_catalog."default",
    billing_address_city character varying COLLATE pg_catalog."default",
    billing_address_state character varying COLLATE pg_catalog."default",
    billing_address_postal_code character varying COLLATE pg_catalog."default",
    billing_address_postal_code_extension character varying COLLATE pg_catalog."default",
    billing_address_country_code character varying COLLATE pg_catalog."default",
    listing_address boolean,
    listing_address_first_name character varying COLLATE pg_catalog."default",
    listing_address_middle_initial character varying COLLATE pg_catalog."default",
    listing_address_last_name character varying COLLATE pg_catalog."default",
    listing_address_company_name character varying COLLATE pg_catalog."default",
    listing_address_address_line_1 character varying COLLATE pg_catalog."default",
    listing_address_address_line_2 character varying COLLATE pg_catalog."default",
    listing_address_city character varying COLLATE pg_catalog."default",
    listing_address_state character varying COLLATE pg_catalog."default",
    listing_address_postal_code character varying COLLATE pg_catalog."default",
    listing_address_postal_code_extension character varying COLLATE pg_catalog."default",
    listing_address_country_code character varying COLLATE pg_catalog."default",
    use_service_address_as_billing_address boolean,
    use_service_address_as_listing_address boolean,
    CONSTRAINT uc_customers1 UNIQUE (tenant_id, rev_customer_id, bandwidth_customer_id, netsapiens_customer_id, e911_customer_id, netsapiens_domain_id, customer_name),
    CONSTRAINT fk_bandwidth_customer FOREIGN KEY (bandwidth_customer_id)
        REFERENCES public.bandwidth_customers (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_e911customer FOREIGN KEY (e911_customer_id)
        REFERENCES public.e911customers (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_revcustomer FOREIGN KEY (rev_customer_id)
        REFERENCES public.revcustomer (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);
CREATE INDEX IF NOT EXISTS idx_cust_id
    ON public.customers USING btree
    (id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_cust_rev_customer_id

-- DROP INDEX IF EXISTS public.idx_cust_rev_customer_id;

CREATE INDEX IF NOT EXISTS idx_cust_rev_customer_id
    ON public.customers USING btree
    (rev_customer_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_customers_bill_period_end_day_hour

-- DROP INDEX IF EXISTS public.idx_customers_bill_period_end_day_hour;

CREATE INDEX IF NOT EXISTS idx_customers_bill_period_end_day_hour
    ON public.customers USING btree
    (customer_bill_period_end_day ASC NULLS LAST, customer_bill_period_end_hour ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_customers_is_active_deleted

-- DROP INDEX IF EXISTS public.idx_customers_is_active_deleted;

CREATE INDEX IF NOT EXISTS idx_customers_is_active
    ON public.customers USING btree
    (is_active ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_customers_name

-- DROP INDEX IF EXISTS public.idx_customers_name;

CREATE INDEX IF NOT EXISTS idx_customers_name
    ON public.customers USING btree
    (customer_name COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS public.customergroups
(
    id serial primary key,
    name character varying(200) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    customergroup_id integer,
    tenant_id integer,
    tenant_name character varying(100) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    subtenant_id integer,
    subtenant_name character varying(100) COLLATE pg_catalog."default",
    child_account character varying(100) COLLATE pg_catalog."default",
    billing_account_number character varying(50) COLLATE pg_catalog."default",
    rate_plan_name character varying COLLATE pg_catalog."default",
    feature_codes character varying COLLATE pg_catalog."default",
    created_by character varying(100) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    is_active boolean,
    customer_names character varying COLLATE pg_catalog."default",
    CONSTRAINT unique_customergroup_tenant UNIQUE (name, tenant_id)
);

CREATE TABLE IF NOT EXISTS public.mobility_feature
(
    id serial primary key,
    soc_code character varying(50) COLLATE pg_catalog."default",
    friendly_name character varying(256) COLLATE pg_catalog."default",
    service_provider_id integer,
    is_retired boolean,
    amount_with_deal_registration numeric(25,4),
    amount_without_deal_registration numeric(25,4),
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean,
    family character varying(100) COLLATE pg_catalog."default",
    CONSTRAINT fk_service_provider_id FOREIGN KEY (service_provider_id)
        REFERENCES public.serviceprovider (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);
CREATE INDEX IF NOT EXISTS idx_mobility_feature_active_retired
    ON public.mobility_feature USING btree
    (id ASC NULLS LAST,is_active ASC NULLS LAST, is_retired ASC NULLS LAST);

CREATE TABLE IF NOT EXISTS public.customer_mobility_feature
(
    id serial primary key,
    service_provider_id integer,
    service_provider character varying(100) COLLATE pg_catalog."default",
    customer_id integer,
    customer_name character varying(255) COLLATE pg_catalog."default",
    mobility_feature_id integer,
    mobility_features text COLLATE pg_catalog."default",
    created_by character varying(100) COLLATE pg_catalog."default" NOT NULL,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean,
    CONSTRAINT fk_mobility_feature_id FOREIGN KEY (mobility_feature_id)
        REFERENCES public.mobility_feature (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL,
    CONSTRAINT fk_service_provider_id FOREIGN KEY (service_provider_id)
        REFERENCES public.serviceprovider (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS public.customer_rate_plan_jasper_carrier_rate_plan (
    id SERIAL PRIMARY KEY,
    jasper_carrier_rate_plan_id INTEGER NOT NULL,
    customer_rate_plan_id INTEGER NOT NULL,
    created_by VARCHAR(100) NOT NULL,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_by VARCHAR(100),
    modified_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_by VARCHAR(100),
    deleted_date TIMESTAMP,
    is_active BOOLEAN NOT NULL,
    CONSTRAINT fk_customer_rate_plan FOREIGN KEY (customer_rate_plan_id)
        REFERENCES public.customerrateplan (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);


CREATE TABLE IF NOT EXISTS public.device_status
(
    id serial primary key,
    status character varying(50) COLLATE pg_catalog."default",
    description character varying(50) COLLATE pg_catalog."default",
    status_color character varying(50) COLLATE pg_catalog."default",
    status_color_code character varying(8) COLLATE pg_catalog."default",
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean,
    allows_api_update boolean,
    display_name character varying(50) COLLATE pg_catalog."default",
    integration_id integer,
    is_active_status boolean,
    should_have_billed_service boolean,
    is_restorable_from_archive boolean,
    is_apply_for_automation_rule boolean,
    status_alias character varying(100) COLLATE pg_catalog."default",
    CONSTRAINT fk_integration_id FOREIGN KEY (integration_id)
        REFERENCES public.integration (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_device_status_composite
    ON public.device_status USING btree
    (id ASC NULLS LAST, description COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default
    WHERE lower(description::text) <> 'suspended'::text;

CREATE INDEX IF NOT EXISTS idx_device_status_description
    ON public.device_status USING btree
    (description COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_device_status_device_status_id
    ON public.device_status USING btree
    (id ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_device_status_diplay_name
    ON public.device_status USING btree
    (display_name COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_device_status_display_name_active
    ON public.device_status USING btree
    (display_name COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default
    WHERE display_name::text = ANY (ARRAY['RestoredFromArchive'::character varying, 'Restored from archive'::character varying]::text[]);

CREATE INDEX IF NOT EXISTS idx_device_status_id_is_active
    ON public.device_status USING btree
    (id ASC NULLS LAST, is_active_status ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_device_status_integration_id
    ON public.device_status USING btree
    (integration_id ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_device_status_is_active_status
    ON public.device_status USING btree
    (is_active_status ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_device_status_should_have_billed_service
    ON public.device_status USING btree
    (should_have_billed_service ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_ds_id
    ON public.device_status USING btree
    (id ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_ds_status
    ON public.device_status USING btree
    (status COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS public.sim_management_communication_plan
(
    id serial primary key,
    communication_plan_name character varying(250) COLLATE pg_catalog."default",
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean,
    service_provider_id integer,
    alias_name character varying(250) COLLATE pg_catalog."default",
    tenant_id integer,
    tenant_name character varying(80) COLLATE pg_catalog."default",
    carrier_rate_plans text COLLATE pg_catalog."default",
    service_provider_name character varying(100) COLLATE pg_catalog."default",
    CONSTRAINT fk_service_provider_id FOREIGN KEY (service_provider_id)
        REFERENCES public.serviceprovider (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS public.revagent
(
    id serial primary key,
    rev_agent_id integer,
    agent_name character varying(250) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    created_by character varying(100) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    deleted_date timestamp without time zone,
    is_active boolean,
    parent_agent_id integer,
    status character varying(50) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    integration_authentication_id integer
);
CREATE INDEX IF NOT EXISTS idx_rev_agent_rev_agent_id
    ON public.revagent USING btree
    (rev_agent_id ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS public.revbillprofile
(
    id serial primary key,
    bill_profile_id integer,
    description character varying(1024) COLLATE pg_catalog."default",
    created_by character varying(100) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp with time zone,
    is_active boolean,
    integration_authentication_id integer
);
CREATE INDEX IF NOT EXISTS idx_rev_bill_profile_bill_profile_id
    ON public.revbillprofile USING btree
    (bill_profile_id ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS public.rev_provider
(
    id serial primary key,
    provider_id integer,
    description character varying(1024) COLLATE pg_catalog."default",
    bill_profile_id integer,
    provider_code character varying(64) COLLATE pg_catalog."default",
    has_cnam_order_type boolean,
    has_conversion_order_type boolean,
    has_deny_order_type boolean,
    has_disconnect_order_type boolean,
    has_e911_order_type boolean,
    has_long_distance_block_order_type boolean,
    has_port_order_type boolean,
    has_restore_order_type boolean,
    has_transfer_order_type boolean,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean,
    integration_authentication_id integer,
    rev_bill_profile_id integer,
    service_provider_id integer,
    service_provider character varying(100) COLLATE pg_catalog."default",
    tenant_id integer,
    CONSTRAINT fk_revprovider_integration_authentication FOREIGN KEY (integration_authentication_id)
        REFERENCES public.integration_authentication (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_revprovider_revbillprofile FOREIGN KEY (rev_bill_profile_id)
        REFERENCES public.revbillprofile (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);
CREATE TABLE IF NOT EXISTS public.rev_service_type
(
    id serial primary key,
    service_type_id integer,
    description character varying(1024) COLLATE pg_catalog."default",
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean,
    integration_authentication_id integer,
    service_provider_id integer,
    service_provider character varying(100) COLLATE pg_catalog."default",
    tenant_id integer,
    CONSTRAINT fk_revservicetype_integrationauthenticationid FOREIGN KEY (integration_authentication_id)
        REFERENCES public.integration_authentication (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);
CREATE INDEX IF NOT EXISTS idx_rev_service_type_id
    ON public.rev_service_type USING btree
    (id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_rev_service_type_id_integration_authentication_id

-- DROP INDEX IF EXISTS public.idx_rev_service_type_id_integration_authentication_id;

CREATE INDEX IF NOT EXISTS idx_rev_service_type_id_integration_authentication_id
    ON public.rev_service_type USING btree
    (service_type_id ASC NULLS LAST, integration_authentication_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_rev_service_type_id_integration_id

-- DROP INDEX IF EXISTS public.idx_rev_service_type_id_integration_id;

CREATE INDEX IF NOT EXISTS idx_rev_service_type_id_integration_id
    ON public.rev_service_type USING btree
    (id ASC NULLS LAST, integration_authentication_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_rev_service_type_integration_authentication_id

-- DROP INDEX IF EXISTS public.idx_rev_service_type_integration_authentication_id;

CREATE INDEX IF NOT EXISTS idx_rev_service_type_integration_authentication_id
    ON public.rev_service_type USING btree
    (integration_authentication_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_rev_service_type_service_type_id

-- DROP INDEX IF EXISTS public.idx_rev_service_type_service_type_id;

CREATE INDEX IF NOT EXISTS idx_rev_service_type_service_type_id
    ON public.rev_service_type USING btree
    (service_type_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_rev_service_type_service_type_id_description

-- DROP INDEX IF EXISTS public.idx_rev_service_type_service_type_id_description;

CREATE INDEX IF NOT EXISTS idx_rev_service_type_service_type_id_description
    ON public.rev_service_type USING btree
    (service_type_id ASC NULLS LAST, description COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default
    WHERE service_type_id IS NULL OR service_type_id = 18;

CREATE TABLE IF NOT EXISTS public.rev_usage_plan_group
(
    id serial primary key,
    usage_plan_group_id integer,
    description character varying(256) COLLATE pg_catalog."default",
    long_description text COLLATE pg_catalog."default",
    rev_active boolean,
    rev_created_by integer,
    rev_created_date timestamp without time zone,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean,
    integration_authentication_id integer,
    CONSTRAINT fk_revusageplangroup_integrationauthentication FOREIGN KEY (integration_authentication_id)
        REFERENCES public.integration_authentication (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);
CREATE TABLE IF NOT EXISTS public.rev_service
(
    id serial primary key,
    rev_customer_id uuid,
    "number" character varying(250) COLLATE pg_catalog."default",
    rev_service_id integer,
    rate_plan_code character varying(255) COLLATE pg_catalog."default",
    activated_date character varying(50) COLLATE pg_catalog."default",
    disconnected_date character varying(50) COLLATE pg_catalog."default",
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean,
    integration_authentication_id integer,
    rev_provider_id integer,
    rev_service_type_id integer,
    rev_usage_plan_group_id integer,
    description text COLLATE pg_catalog."default",
    pro_rate boolean,
    CONSTRAINT fk_revservice_integration_authentication FOREIGN KEY (integration_authentication_id)
        REFERENCES public.integration_authentication (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_revservice_revcustomer FOREIGN KEY (rev_customer_id)
        REFERENCES public.revcustomer (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_revservice_revprovider FOREIGN KEY (rev_provider_id)
        REFERENCES public.rev_provider (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_revservice_revservicetype FOREIGN KEY (rev_service_type_id)
        REFERENCES public.rev_service_type (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_revservice_revusageplangroup FOREIGN KEY (rev_usage_plan_group_id)
        REFERENCES public.rev_usage_plan_group (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);
CREATE INDEX IF NOT EXISTS idx_rev_service_active
    ON public.rev_service USING btree
    (rev_service_id ASC NULLS LAST)
    TABLESPACE pg_default
    WHERE is_active = true;
-- Index: idx_rev_service_composite

-- DROP INDEX IF EXISTS public.idx_rev_service_composite;

CREATE INDEX IF NOT EXISTS idx_rev_service_composite
    ON public.rev_service USING btree
    (rev_service_id ASC NULLS LAST, integration_authentication_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_rev_service_id

-- DROP INDEX IF EXISTS public.idx_rev_service_id;

CREATE INDEX IF NOT EXISTS idx_rev_service_id
    ON public.rev_service USING btree
    (id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_rev_service_integration_authentication_id

-- DROP INDEX IF EXISTS public.idx_rev_service_integration_authentication_id;

CREATE INDEX IF NOT EXISTS idx_rev_service_integration_authentication_id
    ON public.rev_service USING btree
    (integration_authentication_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_rev_service_rev_service_type_id

-- DROP INDEX IF EXISTS public.idx_rev_service_rev_service_type_id;

CREATE INDEX IF NOT EXISTS idx_rev_service_rev_service_type_id
    ON public.rev_service USING btree
    (rev_service_type_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_rev_service_service_id_integration_authentication_id

-- DROP INDEX IF EXISTS public.idx_rev_service_service_id_integration_authentication_id;

CREATE INDEX IF NOT EXISTS idx_rev_service_service_id_integration_authentication_id
    ON public.rev_service USING btree
    (rev_service_id ASC NULLS LAST, integration_authentication_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_rs_activated_date

-- DROP INDEX IF EXISTS public.idx_rs_activated_date;

CREATE INDEX IF NOT EXISTS idx_rs_activated_date
    ON public.rev_service USING btree
    (activated_date COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_rs_rev_service_id

-- DROP INDEX IF EXISTS public.idx_rs_rev_service_id;

CREATE INDEX IF NOT EXISTS idx_rs_rev_service_id
    ON public.rev_service USING btree
    (rev_service_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: ix_rev_service_rev_customer_id

-- DROP INDEX IF EXISTS public.ix_rev_service_rev_customer_id;

CREATE INDEX IF NOT EXISTS ix_rev_service_rev_customer_id
    ON public.rev_service USING btree
    (rev_customer_id ASC NULLS LAST)
    INCLUDE(disconnected_date, integration_authentication_id, rate_plan_code, rev_service_id)
    TABLESPACE pg_default;
-- Index: ix_rev_service_rev_service_id

-- DROP INDEX IF EXISTS public.ix_rev_service_rev_service_id;

CREATE INDEX IF NOT EXISTS ix_rev_service_rev_service_id
    ON public.rev_service USING btree
    (rev_service_id ASC NULLS LAST)
    INCLUDE("number", rev_service_type_id)
    TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS public.device
(
    id serial primary key,
    service_provider_id integer NOT NULL,
    iccid character varying(50) COLLATE pg_catalog."default" NOT NULL,
    imsi character varying(150) COLLATE pg_catalog."default",
    msisdn character varying(150) COLLATE pg_catalog."default",
    imei character varying(150) COLLATE pg_catalog."default",
    eid character varying(50) COLLATE pg_catalog."default",
    device_status_id integer,
    sim_status character varying(50) COLLATE pg_catalog."default",
    carrier_rate_plan_id integer,
    carrier_rate_plan character varying(255) COLLATE pg_catalog."default",
    communication_plan character varying(255) COLLATE pg_catalog."default",
    last_usage_date timestamp without time zone,
    apn character varying(250) COLLATE pg_catalog."default",
    "package" character varying(250) COLLATE pg_catalog."default",
    billing_cycle_end_date timestamp without time zone,
    carrier_cycle_usage bigint,
    ctd_sms_usage bigint,
    ctd_voice_usage bigint,
    ctd_session_count bigint,
    overage_limit_reached boolean,
    overage_limit_override character varying(50) COLLATE pg_catalog."default",
    created_by character varying(100) COLLATE pg_catalog."default" NOT NULL,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    last_activated_date timestamp without time zone,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean NOT NULL,
    date_added timestamp without time zone,
    date_activated timestamp without time zone,
    delta_ctd_data_usage bigint,
    cost_center character varying(250) COLLATE pg_catalog."default",
    username character varying(250) COLLATE pg_catalog."default",
    billing_period_id integer,
    device_description text COLLATE pg_catalog."default",
    ip_address character varying(50) COLLATE pg_catalog."default",
    service_provider_name character varying(300) COLLATE pg_catalog."default",
    customer_cycle_usage bigint,
    soc character varying COLLATE pg_catalog."default"
);
CREATE INDEX IF NOT EXISTS idx_device_carrier_cycle_usage
    ON public.device USING btree
    (carrier_cycle_usage ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_device_device_status_id
    ON public.device USING btree
    (device_status_id ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_device_iccid
    ON public.device USING btree
    (iccid COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_device_iccid_msisdn_eid
    ON public.device USING btree
    (iccid COLLATE pg_catalog."default" ASC NULLS LAST, msisdn COLLATE pg_catalog."default" ASC NULLS LAST, eid COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_device_id
    ON public.device USING btree
    (id ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_device_is_active
    ON public.device USING btree
    (is_active ASC NULLS LAST)
    TABLESPACE pg_default
    WHERE is_active = true;

CREATE INDEX IF NOT EXISTS idx_device_last_activated_date
    ON public.device USING btree
    (last_activated_date ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS idx_device_service_provider_id
    ON public.device USING btree
    (service_provider_id ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS ix_device_serviceprovider_accountnumber_billyear_billmonth_icci
    ON public.device USING btree
    (iccid COLLATE pg_catalog."default" ASC NULLS LAST, service_provider_id ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS ix_device_serviceproviderid_accountnumber
    ON public.device USING btree
    (service_provider_id ASC NULLS LAST)
    INCLUDE(communication_plan, created_date, device_status_id, iccid, id, last_activated_date, msisdn)
    TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS public.mobility_device
(
    id serial primary key,
    service_provider_id integer,
    foundation_account_number character varying(30) COLLATE pg_catalog."default",
    billing_account_number character varying(30) COLLATE pg_catalog."default",
    iccid character varying(50) COLLATE pg_catalog."default",
    imsi character varying(150) COLLATE pg_catalog."default",
    msisdn character varying(150) COLLATE pg_catalog."default",
    imei character varying(150) COLLATE pg_catalog."default",
    device_status_id integer,
    sim_status character varying(50) COLLATE pg_catalog."default",
    carrier_rate_plan_id integer,
    carrier_rate_plan character varying(200) COLLATE pg_catalog."default",
    last_usage_date timestamp without time zone,
    carrier_cycle_usage bigint,
    ctd_sms_usage bigint,
    ctd_voice_usage bigint,
    ctd_session_count bigint,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone,
    last_activated_date timestamp without time zone,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean NOT NULL,
    date_added timestamp without time zone,
    date_activated timestamp without time zone,
    delta_ctd_data_usage bigint,
    billing_period_id integer,
    single_user_code character varying(200) COLLATE pg_catalog."default",
    single_user_code_description character varying(200) COLLATE pg_catalog."default",
    service_zip_code character varying(50) COLLATE pg_catalog."default",
    next_bill_cycle_date timestamp without time zone,
    sms_count integer,
    minutes_used integer,
    data_group_id character varying(50) COLLATE pg_catalog."default",
    pool_id character varying(50) COLLATE pg_catalog."default",
    device_make character varying(50) COLLATE pg_catalog."default",
    device_model character varying(50) COLLATE pg_catalog."default",
    contract_status character varying(50) COLLATE pg_catalog."default",
    ban_status character varying(50) COLLATE pg_catalog."default",
    imei_type_id integer,
    plan_limit_mb numeric(25,4),
    username character varying(150) COLLATE pg_catalog."default",
    technology_type character varying(50) COLLATE pg_catalog."default",
    ip_address character varying(50) COLLATE pg_catalog."default",
    optimization_group_id integer,
    cost_center_1 character varying(250) COLLATE pg_catalog."default",
    soc character varying COLLATE pg_catalog."default"
);
CREATE INDEX IF NOT EXISTS ix_mobility_d_iccid
    ON public.mobility_device USING btree
    (iccid COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE INDEX IF NOT EXISTS ix_mobility_device_carrier_rate_plan_id
    ON public.mobility_device USING btree
    (carrier_rate_plan_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: ix_mobility_device_device_status_id

-- DROP INDEX IF EXISTS public.ix_mobility_device_device_status_id;

CREATE INDEX IF NOT EXISTS ix_mobility_device_device_status_id
    ON public.mobility_device USING btree
    (device_status_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: ix_mobility_device_iccid_service_provider

-- DROP INDEX IF EXISTS public.ix_mobility_device_iccid_service_provider;

CREATE INDEX IF NOT EXISTS ix_mobility_device_iccid_service_provider
    ON public.mobility_device USING btree
    (iccid COLLATE pg_catalog."default" ASC NULLS LAST, service_provider_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: ix_mobility_device_id

-- DROP INDEX IF EXISTS public.ix_mobility_device_id;

CREATE INDEX IF NOT EXISTS ix_mobility_device_id
    ON public.mobility_device USING btree
    (id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: ix_mobility_device_msisdn

-- DROP INDEX IF EXISTS public.ix_mobility_device_msisdn;

CREATE INDEX IF NOT EXISTS ix_mobility_device_msisdn
    ON public.mobility_device USING btree
    (msisdn COLLATE pg_catalog."default" ASC NULLS LAST)
    INCLUDE(is_active)
    TABLESPACE pg_default;
-- Index: ix_mobility_device_service_provider_billing_period_iccid

-- DROP INDEX IF EXISTS public.ix_mobility_device_service_provider_billing_period_iccid;

CREATE INDEX IF NOT EXISTS ix_mobility_device_service_provider_billing_period_iccid
    ON public.mobility_device USING btree
    (iccid COLLATE pg_catalog."default" ASC NULLS LAST, service_provider_id ASC NULLS LAST, billing_period_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: ix_mobility_device_service_provider_id_includes

-- DROP INDEX IF EXISTS public.ix_mobility_device_service_provider_id_includes;

CREATE INDEX IF NOT EXISTS ix_mobility_device_service_provider_id_includes
    ON public.mobility_device USING btree
    (service_provider_id ASC NULLS LAST)
    INCLUDE(created_date, device_status_id, iccid, id, last_activated_date, msisdn);

CREATE TABLE IF NOT EXISTS public.device_tenant
(
    id serial primary key,
    device_id integer NOT NULL,
    tenant_id integer NOT NULL,
    rev_service_id integer,
    customer_rate_plan_id integer,
    customer_data_allocation_mb numeric(25,4),
    customer_rate_pool_id integer,
    account_number character varying(50) COLLATE pg_catalog."default",
    customer_id integer,
    account_number_integration_authentication_id integer,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean NOT NULL,
    customer_rate_pool_name character varying(300) COLLATE pg_catalog."default",
    customer_rate_plan_name character varying(300) COLLATE pg_catalog."default",
    customer_name character varying COLLATE pg_catalog."default",
    CONSTRAINT device_tenant_device_id_tenant_id_key UNIQUE (device_id, tenant_id)
);
CREATE INDEX IF NOT EXISTS idx_device_tenant
    ON public.device_tenant USING btree
    (tenant_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_device_tenant_customer_id

-- DROP INDEX IF EXISTS public.idx_device_tenant_customer_id;

CREATE INDEX IF NOT EXISTS idx_device_tenant_customer_id
    ON public.device_tenant USING btree
    (customer_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_device_tenant_device_customer

-- DROP INDEX IF EXISTS public.idx_device_tenant_device_customer;

CREATE INDEX IF NOT EXISTS idx_device_tenant_device_customer
    ON public.device_tenant USING btree
    (device_id ASC NULLS LAST, customer_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_device_tenant_device_id_tenant_id

-- DROP INDEX IF EXISTS public.idx_device_tenant_device_id_tenant_id;

CREATE INDEX IF NOT EXISTS idx_device_tenant_device_id_tenant_id
    ON public.device_tenant USING btree
    (device_id ASC NULLS LAST, tenant_id ASC NULLS LAST)
    TABLESPACE pg_default
    WHERE is_active IS TRUE;
-- Index: idx_device_tenant_device_rev_service

-- DROP INDEX IF EXISTS public.idx_device_tenant_device_rev_service;

CREATE INDEX IF NOT EXISTS idx_device_tenant_device_rev_service
    ON public.device_tenant USING btree
    (device_id ASC NULLS LAST, rev_service_id ASC NULLS LAST)
    TABLESPACE pg_default
    WHERE is_active = true;
-- Index: idx_device_tenant_did_deleted_false

-- DROP INDEX IF EXISTS public.idx_device_tenant_did_deleted_false;

CREATE INDEX IF NOT EXISTS idx_device_tenant_did_deleted_false
    ON public.device_tenant USING btree
    (device_id ASC NULLS LAST)
    TABLESPACE pg_default
    WHERE is_active = true;
-- Index: idx_device_tenant_is_active

-- DROP INDEX IF EXISTS public.idx_device_tenant_is_active;

CREATE INDEX IF NOT EXISTS idx_device_tenant_is_active
    ON public.device_tenant USING btree
    (is_active ASC NULLS LAST)
    TABLESPACE pg_default
    WHERE is_active = true;

-- Index: idx_device_tenant_rev_service_id

-- DROP INDEX IF EXISTS public.idx_device_tenant_rev_service_id;

CREATE INDEX IF NOT EXISTS idx_device_tenant_rev_service_id
    ON public.device_tenant USING btree
    (rev_service_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_device_tenant_tenant_device

-- DROP INDEX IF EXISTS public.idx_device_tenant_tenant_device;

CREATE INDEX IF NOT EXISTS idx_device_tenant_tenant_device
    ON public.device_tenant USING btree
    (tenant_id ASC NULLS LAST, device_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_device_tenant_tenant_id_is_active

-- DROP INDEX IF EXISTS public.idx_device_tenant_tenant_id_is_active;

CREATE INDEX IF NOT EXISTS idx_device_tenant_tenant_id_is_active
    ON public.device_tenant USING btree
    (tenant_id ASC NULLS LAST, is_active ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_dt_customer_rate_plan_id

-- DROP INDEX IF EXISTS public.idx_dt_customer_rate_plan_id;

CREATE INDEX IF NOT EXISTS idx_dt_customer_rate_plan_id
    ON public.device_tenant USING btree
    (customer_rate_plan_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_dt_customer_rate_pool_id

-- DROP INDEX IF EXISTS public.idx_dt_customer_rate_pool_id;

CREATE INDEX IF NOT EXISTS idx_dt_customer_rate_pool_id
    ON public.device_tenant USING btree
    (customer_rate_pool_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: ix_device_tenant_customer_rate_plan_id_device_id

-- DROP INDEX IF EXISTS public.ix_device_tenant_customer_rate_plan_id_device_id;

CREATE INDEX IF NOT EXISTS ix_device_tenant_customer_rate_plan_id_device_id
    ON public.device_tenant USING btree
    (customer_rate_plan_id ASC NULLS LAST, device_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: ix_device_tenant_customer_rate_pool_id

-- DROP INDEX IF EXISTS public.ix_device_tenant_customer_rate_pool_id;

CREATE INDEX IF NOT EXISTS ix_device_tenant_customer_rate_pool_id
    ON public.device_tenant USING btree
    (customer_rate_pool_id ASC NULLS LAST)
    INCLUDE(customer_data_allocation_mb, customer_rate_plan_id, device_id)
    TABLESPACE pg_default;
-- Index: ix_device_tenant_device_id

-- DROP INDEX IF EXISTS public.ix_device_tenant_device_id;

CREATE INDEX IF NOT EXISTS ix_device_tenant_device_id
    ON public.device_tenant USING btree
    (device_id ASC NULLS LAST)
    INCLUDE(account_number, customer_rate_plan_id, customer_id)
    TABLESPACE pg_default;
-- Index: ix_device_tenant_site_id

-- DROP INDEX IF EXISTS public.ix_device_tenant_site_id;

CREATE INDEX IF NOT EXISTS ix_device_tenant_site_id
    ON public.device_tenant USING btree
    (customer_id ASC NULLS LAST)
    INCLUDE(account_number, customer_rate_plan_id, device_id)
    TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS public.mobility_device_tenant
(
    id serial primary key ,
    mobility_device_id integer NOT NULL,
    tenant_id integer NOT NULL,
    rev_service_id integer,
    customer_rate_plan_id integer,
    customer_data_allocation_mb numeric(25,4),
    customer_rate_pool_id integer,
    account_number character varying(50) ,
    customer_id integer,
	customer_name varchar,
    account_number_integration_authentication_id integer,
    created_by character varying(100) ,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) ,
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) ,
    deleted_date timestamp without time zone,
    is_active boolean NOT NULL,
    service_provider_name character varying(300) ,
    customer_rate_plan_name character varying(300),
    customer_rate_pool_name character varying(300)
);

CREATE INDEX IF NOT EXISTS idx_mobility_device_tenant_customer_id_rate_plan_id
    ON public.mobility_device_tenant USING btree
    (customer_id ASC NULLS LAST, customer_rate_plan_id ASC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_mobility_device_tenant_device_id
    ON public.mobility_device_tenant USING btree
    (mobility_device_id ASC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_mobility_device_tenant_id
    ON public.mobility_device_tenant USING btree
    (mobility_device_id ASC NULLS LAST, tenant_id ASC NULLS LAST);

CREATE INDEX IF NOT EXISTS idx_mobility_device_tenant_rev_service_id
    ON public.mobility_device_tenant USING btree
    (rev_service_id ASC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_mobility_device_tenant_tenant_id
    ON public.mobility_device_tenant USING btree
    (tenant_id ASC NULLS LAST);

CREATE INDEX IF NOT EXISTS idx_mobility_device_tenant__plan_id_mobility_device_id_id
    ON public.mobility_device_tenant USING btree (customer_rate_plan_id ASC NULLS LAST, mobility_device_id ASC NULLS LAST, id ASC NULLS LAST);

-- Index for CustomerRatePoolId with included columns
CREATE INDEX IF NOT EXISTS idx_mobility_device_tenant_customer_rate_pool_id
    ON public.mobility_device_tenant USING btree (customer_rate_pool_id ASC NULLS LAST)
    INCLUDE (customer_data_allocation_mb, customer_rate_plan_id, mobility_device_id);


-- Index for TenantId and CustomerRatePoolId with included columns
CREATE INDEX IF NOT EXISTS idx_mobility_device_tenant_tenant_id_customer_rate_pool_id
    ON public.mobility_device_tenant USING btree (tenant_id ASC NULLS LAST, customer_rate_pool_id ASC NULLS LAST)
    INCLUDE (customer_data_allocation_mb, customer_rate_plan_id, mobility_device_id);

CREATE TABLE IF NOT EXISTS public.sim_management_inventory
(
    id serial primary key,
    device_id integer,
    mobility_device_id integer,
    dt_id integer,
    mdt_id integer,
    service_provider_id integer,
    service_provider_display_name character varying(100) COLLATE pg_catalog."default",
    integration_id integer,
    billing_account_number character varying(30) COLLATE pg_catalog."default",
    foundation_account_number character varying(30) COLLATE pg_catalog."default",
    iccid character varying(50) COLLATE pg_catalog."default",
    imsi character varying(150) COLLATE pg_catalog."default",
    msisdn character varying(150) COLLATE pg_catalog."default",
    imei character varying(150) COLLATE pg_catalog."default",
    eid character varying(50) COLLATE pg_catalog."default",
    customer_id integer,
    customer_name character varying(350) COLLATE pg_catalog."default",
    parent_customer_id integer,
    rev_customer_id character varying(50) COLLATE pg_catalog."default",
    rev_customer_name character varying(250) COLLATE pg_catalog."default",
    rev_parent_customer_id character varying COLLATE pg_catalog."default",
    device_status_id integer,
    sim_status character varying(50) COLLATE pg_catalog."default",
    carrier_cycle_usage_bytes bigint,
    carrier_cycle_usage_mb bigint,
    date_added timestamp without time zone,
    date_activated timestamp without time zone,
    account_number character varying(50) COLLATE pg_catalog."default",
    carrier_rate_plan_id integer,
    carrier_rate_plan_name character varying(200) COLLATE pg_catalog."default",
    communication_plan_id integer,
    communication_plan character varying(255) COLLATE pg_catalog."default",
    customer_cycle_usage_mb bigint,
    customer_rate_pool_id integer,
    customer_rate_pool_name character varying(200) COLLATE pg_catalog."default",
    customer_rate_plan_id integer,
    customer_rate_plan_name character varying(100) COLLATE pg_catalog."default",
    customer_rate_plan_code character varying(50) COLLATE pg_catalog."default",
    sms_count integer,
    minutes_used integer,
    username character varying(150) COLLATE pg_catalog."default",
    is_active_status boolean,
    tenant_id integer,
    ip_address character varying(50) COLLATE pg_catalog."default",
    service_zip_code character varying(50) COLLATE pg_catalog."default",
    rate_plan_soc character varying(200) COLLATE pg_catalog."default",
    rate_plan_soc_description character varying(200) COLLATE pg_catalog."default",
    data_group_id character varying(50) COLLATE pg_catalog."default",
    pool_id character varying COLLATE pg_catalog."default",
    next_bill_cycle_date timestamp without time zone,
    device_make character varying(50) COLLATE pg_catalog."default",
    device_model character varying(50) COLLATE pg_catalog."default",
    contract_status character varying(50) COLLATE pg_catalog."default",
    ban_status character varying(50) COLLATE pg_catalog."default",
    imei_type_id integer,
    plan_limit_mb numeric(25,4),
    customer_data_allocation_mb numeric(25,4),
    billing_cycle_start_date timestamp without time zone,
    billing_cycle_end_date timestamp without time zone,
    customer_rate_plan_mb bigint,
    customer_rate_plan_allows_sim_pooling boolean,
    carrier_rate_plan_mb bigint,
    telegence_features text COLLATE pg_catalog."default",
    ebonding_features text COLLATE pg_catalog."default",
    effective_date timestamp without time zone,
    last_usage_date timestamp without time zone,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    last_activated_date timestamp without time zone,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean,
    cost_center character varying(255) COLLATE pg_catalog."default",
    last_used_date timestamp without time zone,
    soc character varying COLLATE pg_catalog."default",
    billing_period_id integer,
    ctd_sms_usage bigint,
    ctd_session_count bigint,
    package varchar NULL,
	overage_limit_reached bool NULL,
	overage_limit_override varchar NULL,
	delta_ctd_data_usage int NULL,
	ctd_voice_usage int NULL,
	rev_service_id int NULL,
	account_number_integration_authentication_id int NULL,
	technology_type varchar NULL,
	optimization_group_id int NULL,
	device_description text NULL,
	carrier_rate_plan_display_rate numeric NULL,
	rev_vw_device_status varchar(255) NULL
);
-- Index: indx_is_active_modified_date_desc

-- DROP INDEX IF EXISTS public.indx_is_active_modified_date_desc;

CREATE INDEX IF NOT EXISTS indx_is_active_modified_date_desc
    ON public.sim_management_inventory USING btree
    (is_active ASC NULLS LAST, modified_date DESC NULLS FIRST)
    TABLESPACE pg_default;
-- Index: indx_isactive

-- DROP INDEX IF EXISTS public.indx_isactive;

CREATE INDEX IF NOT EXISTS indx_isactive
    ON public.sim_management_inventory USING btree
    (is_active ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: indx_sim_management_inventory_composite

-- DROP INDEX IF EXISTS public.indx_sim_management_inventory_composite;

CREATE INDEX IF NOT EXISTS indx_sim_management_inventory_composite
    ON public.sim_management_inventory USING btree
    (mobility_device_id ASC NULLS LAST, service_provider_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: indx_sim_management_inventory_device_iccid

-- DROP INDEX IF EXISTS public.indx_sim_management_inventory_device_iccid;

CREATE INDEX IF NOT EXISTS indx_sim_management_inventory_device_iccid
    ON public.sim_management_inventory USING btree
    (device_id ASC NULLS LAST, iccid COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: indx_sim_management_inventory_device_status_id

-- DROP INDEX IF EXISTS public.indx_sim_management_inventory_device_status_id;

CREATE INDEX IF NOT EXISTS indx_sim_management_inventory_device_status_id
    ON public.sim_management_inventory USING btree
    (device_status_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: indx_sim_management_inventory_iccid

-- DROP INDEX IF EXISTS public.indx_sim_management_inventory_iccid;

CREATE INDEX IF NOT EXISTS indx_sim_management_inventory_iccid
    ON public.sim_management_inventory USING btree
    (iccid COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;

-- Index: indx_sim_management_inventory_last_activated_date

-- DROP INDEX IF EXISTS public.indx_sim_management_inventory_last_activated_date;

CREATE INDEX IF NOT EXISTS indx_sim_management_inventory_last_activated_date
    ON public.sim_management_inventory USING btree
    (last_activated_date ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: indx_sim_management_inventory_mid_active

-- DROP INDEX IF EXISTS public.indx_sim_management_inventory_mid_active;

CREATE INDEX IF NOT EXISTS indx_sim_management_inventory_mid_active
    ON public.sim_management_inventory USING btree
    (mobility_device_id ASC NULLS LAST)
    TABLESPACE pg_default
    WHERE is_active = true;
-- Index: indx_sim_management_inventory_modified_date

-- DROP INDEX IF EXISTS public.indx_sim_management_inventory_modified_date;

CREATE INDEX IF NOT EXISTS indx_sim_management_inventory_modified_date
    ON public.sim_management_inventory USING btree
    (modified_date DESC NULLS FIRST)
    TABLESPACE pg_default;
-- Index: indx_sim_management_inventory_service_provider_billing_period

-- DROP INDEX IF EXISTS public.indx_sim_management_inventory_service_provider_billing_period;

CREATE INDEX IF NOT EXISTS indx_sim_management_inventory_service_provider_billing_period
    ON public.sim_management_inventory USING btree
    (service_provider_id ASC NULLS LAST, billing_period_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: indx_sim_management_inventory_sim_status

-- DROP INDEX IF EXISTS public.indx_sim_management_inventory_sim_status;

CREATE INDEX IF NOT EXISTS indx_sim_management_inventory_sim_status
    ON public.sim_management_inventory USING btree
    (sim_status COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: indx_sim_mgmt_inventory_date_activated

-- DROP INDEX IF EXISTS public.indx_sim_mgmt_inventory_date_activated;

CREATE INDEX IF NOT EXISTS indx_sim_mgmt_inventory_date_activated
    ON public.sim_management_inventory USING btree
    (date_activated ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: indx_smi_carrier_rate_plan_id

-- DROP INDEX IF EXISTS public.indx_smi_carrier_rate_plan_id;

CREATE INDEX IF NOT EXISTS indx_smi_carrier_rate_plan_id
    ON public.sim_management_inventory USING btree
    (carrier_rate_plan_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: indx_smi_customer_rate_plan_id

-- DROP INDEX IF EXISTS public.indx_smi_customer_rate_plan_id;

CREATE INDEX IF NOT EXISTS indx_smi_customer_rate_plan_id
    ON public.sim_management_inventory USING btree
    (customer_rate_plan_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: indx_smi_device_id

-- DROP INDEX IF EXISTS public.indx_smi_device_id;

CREATE INDEX IF NOT EXISTS indx_smi_device_id
    ON public.sim_management_inventory USING btree
    (device_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: indx_smi_device_status_id

-- DROP INDEX IF EXISTS public.indx_smi_device_status_id;

CREATE INDEX IF NOT EXISTS indx_smi_device_status_id
    ON public.sim_management_inventory USING btree
    (device_status_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: indx_smi_iccid

-- DROP INDEX IF EXISTS public.indx_smi_iccid;

CREATE INDEX IF NOT EXISTS indx_smi_iccid
    ON public.sim_management_inventory USING btree
    (iccid COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;


CREATE INDEX IF NOT EXISTS indx_smi_is_active
    ON public.sim_management_inventory USING btree
    (is_active ASC NULLS LAST)
    TABLESPACE pg_default;

-- Index: indx_smi_last_activated_date

-- DROP INDEX IF EXISTS public.indx_smi_last_activated_date;

CREATE INDEX IF NOT EXISTS indx_smi_last_activated_date
    ON public.sim_management_inventory USING btree
    (last_activated_date ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: indx_smi_mobility_device_id

-- DROP INDEX IF EXISTS public.indx_smi_mobility_device_id;

CREATE INDEX IF NOT EXISTS indx_smi_mobility_device_id
    ON public.sim_management_inventory USING btree
    (mobility_device_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: indx_smi_msisdn_service_provider

-- DROP INDEX IF EXISTS public.indx_smi_msisdn_service_provider;

CREATE INDEX IF NOT EXISTS indx_smi_msisdn_service_provider
    ON public.sim_management_inventory USING btree
    (msisdn COLLATE pg_catalog."default" ASC NULLS LAST, service_provider_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: indx_smi_service_provider_id

-- DROP INDEX IF EXISTS public.indx_smi_service_provider_id;

CREATE INDEX IF NOT EXISTS indx_smi_service_provider_id
    ON public.sim_management_inventory USING btree
    (service_provider_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: ix_sim_management_inventory_device_id

-- DROP INDEX IF EXISTS public.ix_sim_management_inventory_device_id;

CREATE INDEX IF NOT EXISTS ix_sim_management_inventory_device_id
    ON public.sim_management_inventory USING btree
    (device_id ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS public.service_provider_tenant_configuration
(
    id serial primary key,
    service_provider_id integer,
    tenant_id integer,
    rev_product_type_id integer,
    created_by character varying(100) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean,
    sms_rev_product_type_id integer,
    overage_rev_product_type_id integer,
    rev_product_id integer,
    sms_rev_product_id integer,
    overage_rev_product_id integer,
    service_provider_name character varying(80) COLLATE pg_catalog."default",
    CONSTRAINT uc_serviceprovidertenantconfiguration_serviceprovider_tenant UNIQUE (service_provider_id, tenant_id),
    CONSTRAINT fk_serviceprovidertenantconfiguration_serviceprovider FOREIGN KEY (service_provider_id)
        REFERENCES public.serviceprovider (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL
);
CREATE TABLE IF NOT EXISTS public.sim_management_bulk_change_type
(
    id serial primary key,
    code character varying(50) COLLATE pg_catalog."default",
    display_name character varying(100) COLLATE pg_catalog."default",
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean,
    CONSTRAINT unique_id_sim_management_bulk_change_type UNIQUE (id, display_name)
);

CREATE TABLE IF NOT EXISTS public.sim_management_bulk_change
(
    id bigserial primary key,
    service_provider_id integer,
    service_provider character varying(80) COLLATE pg_catalog."default",
    tenant_id integer,
    change_request_type_id integer,
    change_request_type character varying(255) COLLATE pg_catalog."default",
    status character varying(50) COLLATE pg_catalog."default",
    uploaded integer,
    customer_id integer,
    customer_name character varying(255) COLLATE pg_catalog."default",
    app_file_id integer,
    processed_date timestamp without time zone,
    processed_by character varying(50) COLLATE pg_catalog."default",
    created_by character varying(100) COLLATE pg_catalog."default" NOT NULL,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean,
    errors integer,
    success integer,
    CONSTRAINT fk_devicebulkchange_changerequesttype FOREIGN KEY (change_request_type_id)
        REFERENCES public.sim_management_bulk_change_type (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL,
    CONSTRAINT fk_devicebulkchange_serviceprovider FOREIGN KEY (service_provider_id)
        REFERENCES public.serviceprovider (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS ix_device_bulk_change_change_request_type_id
    ON public.sim_management_bulk_change USING btree
    (change_request_type_id ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS public.m2m_device_change
(
    id bigserial primary key,
    bulk_change_id bigint NOT NULL,
    iccid character varying(50) COLLATE pg_catalog."default" NOT NULL,
    msisdn character varying(50) COLLATE pg_catalog."default",
    ip_address character varying(50) COLLATE pg_catalog."default",
    change_request text COLLATE pg_catalog."default",
    device_id integer,
    is_processed boolean NOT NULL,
    has_errors boolean NOT NULL,
    status character varying(50) COLLATE pg_catalog."default" NOT NULL,
    status_details text COLLATE pg_catalog."default",
    processed_date timestamp with time zone,
    processed_by character varying(50) COLLATE pg_catalog."default",
    created_by character varying(100) COLLATE pg_catalog."default" NOT NULL,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp with time zone,
    is_active boolean NOT NULL,
    device_change_request_id bigint
);
CREATE INDEX IF NOT EXISTS idx_m2m_device_change_bulkchangeid
    ON public.m2m_device_change USING btree
    (bulk_change_id ASC NULLS LAST)
    INCLUDE(device_change_request_id, device_id, iccid, is_processed, status)
    TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS public.mobility_device_change
(
    id bigserial primary key,
    bulk_change_id bigint NOT NULL,
    subscriber_number character varying(50) COLLATE pg_catalog."default",
    change_request text COLLATE pg_catalog."default",
    device_id integer,
    is_processed boolean NOT NULL,
    has_errors boolean NOT NULL,
    status character varying(50) COLLATE pg_catalog."default" NOT NULL,
    status_details text COLLATE pg_catalog."default",
    processed_date timestamp with time zone,
    processed_by character varying(50) COLLATE pg_catalog."default",
    created_by character varying(100) COLLATE pg_catalog."default" NOT NULL,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp with time zone,
    is_active boolean NOT NULL,
    iccid character varying(50) COLLATE pg_catalog."default",
    additional_step_status character varying(50) COLLATE pg_catalog."default",
    additional_step_details text COLLATE pg_catalog."default",
    ip_address character varying(50) COLLATE pg_catalog."default",
    device_change_request_id bigint
);
CREATE INDEX IF NOT EXISTS idx_mobility_device_change_bulkchangeid
    ON public.mobility_device_change USING btree
    (bulk_change_id ASC NULLS LAST)
    INCLUDE(device_change_request_id, iccid, is_processed, status, subscriber_number)
    TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS public.sim_management_bulk_change_request
(
    id bigserial primary key,
    m2m_id integer,
    mobility_id integer,
    bulk_change_id bigint,
    iccid character varying(60) COLLATE pg_catalog."default",
    msisdn character varying(60) COLLATE pg_catalog."default",
    subscriber_number character varying(50) COLLATE pg_catalog."default",
    ip_address character varying(60) COLLATE pg_catalog."default",
    change_request text COLLATE pg_catalog."default",
    device_id integer,
    sim_management_inventory_id integer,
    is_processed boolean,
    has_errors boolean,
    status character varying(50) COLLATE pg_catalog."default",
    status_details text COLLATE pg_catalog."default",
    processed_date timestamp without time zone,
    processed_by character varying(50) COLLATE pg_catalog."default",
    created_by character varying(50) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(50) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean,
    device_change_request_id integer,
    request_created_date timestamp without time zone,
    request_created_by character varying(50) COLLATE pg_catalog."default",
    request_modified_by timestamp without time zone,
    request_modified_date timestamp without time zone,
    additional_step_status character varying(50) COLLATE pg_catalog."default",
    additional_step_details text COLLATE pg_catalog."default",
    tenant_id integer,
    tenant_name character varying(50) COLLATE pg_catalog."default",
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    uploaded character varying COLLATE pg_catalog."default",
    errors character varying COLLATE pg_catalog."default",
    sucess character varying COLLATE pg_catalog."default",
    CONSTRAINT fk_bulk_change_id FOREIGN KEY (bulk_change_id)
        REFERENCES public.sim_management_bulk_change (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL,
    CONSTRAINT fk_sim_management_inventory_id FOREIGN KEY (sim_management_inventory_id)
        REFERENCES public.sim_management_inventory (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_smi_m2m_device_change_bulkchangeid
    ON public.sim_management_bulk_change_request USING btree
    (bulk_change_id ASC NULLS LAST)
    INCLUDE(device_change_request_id, device_id, iccid, is_processed, status);
CREATE INDEX IF NOT EXISTS idx_smimobility_device_change_bulkchangeid
    ON public.sim_management_bulk_change_request USING btree
    (bulk_change_id ASC NULLS LAST)
    INCLUDE(device_change_request_id, iccid, is_processed, status, subscriber_number);

CREATE TABLE IF NOT EXISTS public.sim_management_bulk_change_log
(
    id bigserial primary key,
    bulk_change_id bigint,
    bulk_change_request_id bigint,
    m2m_device_change_id bigint,
    mobility_device_change_id bigint,
    lnp_device_change_id bigint,
    log_entry_description text COLLATE pg_catalog."default",
    request_text text COLLATE pg_catalog."default",
    has_errors boolean,
    response_status character varying(50) COLLATE pg_catalog."default",
    response_text text COLLATE pg_catalog."default",
    error_text text COLLATE pg_catalog."default",
    processed_date timestamp without time zone,
    processed_by character varying(50) COLLATE pg_catalog."default",
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean,
    CONSTRAINT fk_bulk_change_id FOREIGN KEY (bulk_change_id)
        REFERENCES public.sim_management_bulk_change (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS ix_device_bulk_change_log_bulk_change_id
    ON public.sim_management_bulk_change_log USING btree
    (bulk_change_id ASC NULLS LAST)
    INCLUDE(log_entry_description, processed_date, request_text, response_status);
CREATE INDEX IF NOT EXISTS ix_device_bulk_change_log_response_text_btree
    ON public.sim_management_bulk_change_log USING btree
    (response_text COLLATE pg_catalog."default" ASC NULLS LAST);

CREATE TABLE IF NOT EXISTS public.sim_management_bulk_change_type_service_provider
(
    id serial primary key,
    integration_id integer,
    service_provider_id integer,
    service_provider character varying(50) COLLATE pg_catalog."default",
    change_request_type_id integer,
    change_type character varying(100) COLLATE pg_catalog."default",
	is_active boolean null,
	CONSTRAINT fk_service_provider FOREIGN KEY (service_provider_id)
		REFERENCES serviceprovider(id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL
);



CREATE TABLE IF NOT EXISTS public.sim_management_carrier_feature_codes_uat
(
    id serial primary key,
    service_provider_id integer,
    service_provider_name character varying(100) COLLATE pg_catalog."default",
    customer_id integer,
    customer_name character varying(255) COLLATE pg_catalog."default",
    feature_codes text COLLATE pg_catalog."default",
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean,
    soc_codes text COLLATE pg_catalog."default"
);

CREATE TABLE IF NOT EXISTS public.sim_management_inventory_action_history
(
    id serial primary key,
    service_provider_id integer,
    sim_management_inventory_id integer,
    bulk_change_id bigint,
    uploaded_file_id integer,
    iccid character varying(50) COLLATE pg_catalog."default",
    msisdn character varying(150) COLLATE pg_catalog."default",
    previous_value character varying(250) COLLATE pg_catalog."default",
    current_value character varying(250) COLLATE pg_catalog."default",
    changed_field character varying(250) COLLATE pg_catalog."default",
    change_event_type character varying(250) COLLATE pg_catalog."default",
    date_of_change timestamp without time zone,
    changed_by character varying(100) COLLATE pg_catalog."default",
    username character varying(150) COLLATE pg_catalog."default",
    customer_account_name character varying(500) COLLATE pg_catalog."default",
    customer_account_number character varying(50) COLLATE pg_catalog."default",
    tenant_id integer,
    is_active boolean,
    service_provider character varying(100) COLLATE pg_catalog."default",
    tenant_name character varying(100) COLLATE pg_catalog."default",
    m2m_device_id integer,
    mobility_device_id integer,
    CONSTRAINT fk_bulk_change_id FOREIGN KEY (bulk_change_id)
        REFERENCES public.sim_management_bulk_change (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL,
    CONSTRAINT fk_service_provider_id FOREIGN KEY (service_provider_id)
        REFERENCES public.serviceprovider (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL,
    CONSTRAINT fk_sim_management_inventory_id FOREIGN KEY (sim_management_inventory_id)
        REFERENCES public.sim_management_inventory (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_deviceactionhistory_m2m_tenant_date1
    ON public.sim_management_inventory_action_history USING btree
    (m2m_device_id ASC NULLS LAST, tenant_id ASC NULLS LAST, date_of_change DESC NULLS FIRST)
    INCLUDE(current_value);
CREATE INDEX IF NOT EXISTS idx_deviceactionhistory_mobility_tenant_date1
    ON public.sim_management_inventory_action_history USING btree
    (mobility_device_id ASC NULLS LAST, tenant_id ASC NULLS LAST, date_of_change DESC NULLS FIRST)
    INCLUDE(current_value);

CREATE TABLE IF NOT EXISTS public.device_action_history
(
    id serial primary key,
    sim_management_inventory_id integer,
    m2m_device_id integer,
    mobility_device_id integer,
    service_provider_id integer,
    service_provider character varying(100) COLLATE pg_catalog."default",
    bulk_change_id bigint,
    uploaded_file_id integer,
    iccid character varying(50) COLLATE pg_catalog."default",
    msisdn character varying(150) COLLATE pg_catalog."default",
    previous_value character varying(250) COLLATE pg_catalog."default",
    current_value character varying(250) COLLATE pg_catalog."default",
    changed_field character varying(250) COLLATE pg_catalog."default",
    change_event_type character varying(250) COLLATE pg_catalog."default",
    date_of_change timestamp without time zone,
    changed_by character varying(100) COLLATE pg_catalog."default",
    username character varying(150) COLLATE pg_catalog."default",
    customer_account_name character varying(500) COLLATE pg_catalog."default",
    customer_account_number character varying(50) COLLATE pg_catalog."default",
    tenant_id integer,
    is_active boolean,
    CONSTRAINT fk_bulk_change_id FOREIGN KEY (bulk_change_id)
        REFERENCES public.sim_management_bulk_change (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL,
    CONSTRAINT fk_service_provider_id FOREIGN KEY (service_provider_id)
        REFERENCES public.serviceprovider (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL,
    CONSTRAINT fk_sim_management_inventory_id FOREIGN KEY (sim_management_inventory_id)
        REFERENCES public.sim_management_inventory (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_deviceactionhistory_m2m_tenant_date
    ON public.device_action_history USING btree
    (m2m_device_id ASC NULLS LAST, tenant_id ASC NULLS LAST, date_of_change DESC NULLS FIRST)
    INCLUDE(current_value);
CREATE INDEX IF NOT EXISTS idx_deviceactionhistory_mobility_tenant_date
    ON public.device_action_history USING btree
    (mobility_device_id ASC NULLS LAST, tenant_id ASC NULLS LAST, date_of_change DESC NULLS FIRST)
    INCLUDE(current_value);

CREATE TABLE IF NOT EXISTS public.sim_order_form
(
    id serial primary key,
    company character varying(255) COLLATE pg_catalog."default",
    contact_name character varying(255) COLLATE pg_catalog."default",
    email character varying(255) COLLATE pg_catalog."default",
    country character varying(255) COLLATE pg_catalog."default",
    shipping_address character varying(255) COLLATE pg_catalog."default",
    special_instructions text COLLATE pg_catalog."default",
    expedite character varying(255) COLLATE pg_catalog."default",
    sim_info json,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    created_by character varying(255) COLLATE pg_catalog."default",
    static_ip boolean
);

CREATE TABLE IF NOT EXISTS public.rev_product_type
(
    id serial primary key,
    product_type_id integer,
    product_type_code character varying(512) COLLATE pg_catalog."default",
    description character varying(1024) COLLATE pg_catalog."default",
    tax_class_id character varying(64) COLLATE pg_catalog."default",
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean,
    integration_authentication_id integer,
    CONSTRAINT fk_integration_authentication_id FOREIGN KEY (integration_authentication_id)
        REFERENCES public.integration_authentication (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_rev_product_type_integration_authentication_id
    ON public.rev_product_type USING btree
    (integration_authentication_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_rev_product_type_product_type_code

-- DROP INDEX IF EXISTS public.idx_rev_product_type_product_type_code;

CREATE INDEX IF NOT EXISTS idx_rev_product_type_product_type_code
    ON public.rev_product_type USING btree
    (product_type_code COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_rev_product_type_product_type_id

-- DROP INDEX IF EXISTS public.idx_rev_product_type_product_type_id;

CREATE INDEX IF NOT EXISTS idx_rev_product_type_product_type_id
    ON public.rev_product_type USING btree
    (product_type_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_rev_product_type_recurring

-- DROP INDEX IF EXISTS public.idx_rev_product_type_recurring;

CREATE INDEX IF NOT EXISTS idx_rev_product_type_recurring
    ON public.rev_product_type USING btree
    (product_type_code COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default
    WHERE product_type_code::text ~~ 'RECURRING_%'::text;

CREATE TABLE IF NOT EXISTS public.rev_product
(
    id serial primary key,
    product_id integer,
    product_type_id integer,
    description character varying(1024) COLLATE pg_catalog."default",
    code1 character varying(1024) COLLATE pg_catalog."default",
    code2 character varying(1024) COLLATE pg_catalog."default",
    rate character varying COLLATE pg_catalog."default",
    cost character varying COLLATE pg_catalog."default",
    buy_rate character varying COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    created_by character varying(100) COLLATE pg_catalog."default",
    creates_order boolean,
    provider_id integer,
    bills_in_arrears boolean,
    prorates boolean,
    customer_class character varying(64) COLLATE pg_catalog."default",
    long_description character varying(1024) COLLATE pg_catalog."default",
    ledger_code character varying(64) COLLATE pg_catalog."default",
    free_months integer,
    automatic_expiration_months integer,
    order_completion_billing boolean,
    tax_class_id character varying(64) COLLATE pg_catalog."default",
    wholesale_description character varying(512) COLLATE pg_catalog."default",
    msisdn character varying(150) COLLATE pg_catalog."default",
    iccid character varying(150) COLLATE pg_catalog."default",
    imei character varying(150) COLLATE pg_catalog."default",
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean,
    integration_authentication_id integer,
    rev_provider_id integer,
    tenant_id integer,
    service_provider_id integer,
    service_provider character varying(100) COLLATE pg_catalog."default",
    CONSTRAINT fk_revproduct_integration_authentication FOREIGN KEY (integration_authentication_id)
        REFERENCES public.integration_authentication (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL,
    CONSTRAINT fk_revproduct_revprovider FOREIGN KEY (rev_provider_id)
        REFERENCES public.rev_provider (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL,
    CONSTRAINT fk_service_provider_id FOREIGN KEY (service_provider_id)
        REFERENCES public.serviceprovider (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_rev_product_integration_authentication_id
    ON public.rev_product USING btree
    (integration_authentication_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_rev_product_product_id

-- DROP INDEX IF EXISTS public.idx_rev_product_product_id;

CREATE INDEX IF NOT EXISTS idx_rev_product_product_id
    ON public.rev_product USING btree
    (product_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_rev_product_product_id_integration_authentication_id

-- DROP INDEX IF EXISTS public.idx_rev_product_product_id_integration_authentication_id;

CREATE INDEX IF NOT EXISTS idx_rev_product_product_id_integration_authentication_id
    ON public.rev_product USING btree
    (product_id ASC NULLS LAST, integration_authentication_id ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS public.rev_package
(
    id serial primary key,
    package_id character varying COLLATE pg_catalog."default",
    provider_id integer,
    currency_code character varying(50) COLLATE pg_catalog."default",
    description character varying(1024) COLLATE pg_catalog."default",
    description_on_bill character varying(1024) COLLATE pg_catalog."default",
    long_description character varying(1024) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    created_by character varying(100) COLLATE pg_catalog."default",
    usage_plan_group_id integer,
    service_type_id integer,
    package_category_id character varying COLLATE pg_catalog."default",
    exempt_from_spiff_commission boolean,
    restrict_class_flag boolean,
    class character varying(64) COLLATE pg_catalog."default",
    restrict_bill_profile_flag boolean,
    bill_profile_id integer,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean DEFAULT true,
    integration_authentication_id integer,
    rev_provider_id integer,
    service_provider_id integer,
    service_provider character varying(100) COLLATE pg_catalog."default",
    tenant_id integer,
    rev_provider_name character varying(255) COLLATE pg_catalog."default",
    CONSTRAINT fk_revproduct_integration_authentication FOREIGN KEY (integration_authentication_id)
        REFERENCES public.integration_authentication (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL,
    CONSTRAINT fk_revproduct_revprovider FOREIGN KEY (rev_provider_id)
        REFERENCES public.rev_provider (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL,
    CONSTRAINT fk_service_provider_id FOREIGN KEY (service_provider_id)
        REFERENCES public.serviceprovider (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS public.optimization_type
(
    id bigserial primary key,
    name character varying(100) COLLATE pg_catalog."default",
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean
);

CREATE TABLE IF NOT EXISTS public.optimization_status
(
    id serial primary key,
    display_name character varying(50) COLLATE pg_catalog."default",
    display_order integer,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean DEFAULT true
);

CREATE TABLE IF NOT EXISTS public.mobility_device_usage_aggregate
(
    id serial primary key,
    service_provider_id integer,
    service_provider character varying(100) COLLATE pg_catalog."default",
    foundation_account_number character varying(50) COLLATE pg_catalog."default" NOT NULL,
    billing_account_number character varying(50) COLLATE pg_catalog."default",
    data_group_id character varying(50) COLLATE pg_catalog."default",
    pool_id character varying(50) COLLATE pg_catalog."default",
    data_usage bigint,
    voice_usage integer,
    sms_usage integer,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_date timestamp without time zone,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    is_active boolean,
    data_total bigint,
    tenant_id integer,
    CONSTRAINT fk_service_provider_id FOREIGN KEY (service_provider_id)
        REFERENCES public.serviceprovider (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS public.imei_master
(
    id serial primary key,
    manufacturer character varying(100) COLLATE pg_catalog."default",
    model character varying(255) COLLATE pg_catalog."default",
    from_imei character varying(60) COLLATE pg_catalog."default",
    to_imei character varying(60) COLLATE pg_catalog."default",
    device_type character varying(250) COLLATE pg_catalog."default",
    sim_type character varying(255) COLLATE pg_catalog."default",
    att_certified boolean,
    device_common_name character varying(255) COLLATE pg_catalog."default",
    device_marketing_name character varying(255) COLLATE pg_catalog."default",
    network_type character varying(100) COLLATE pg_catalog."default",
    ran_type character varying(100) COLLATE pg_catalog."default",
    nsdev boolean,
    volte_capable boolean,
    service_provider_id integer,
    service_provider integer,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(50) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(50) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean,
    tenant_id integer,
    CONSTRAINT fk_service_provider_id FOREIGN KEY (service_provider_id)
        REFERENCES public.serviceprovider (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS public.device_history
(
    device_history_id bigserial primary key,
    changed_date timestamp without time zone,
    id integer,
    service_provider_id integer,
    iccid character varying(50) COLLATE pg_catalog."default",
    imsi character varying(150) COLLATE pg_catalog."default",
    msisdn character varying(150) COLLATE pg_catalog."default",
    imei character varying(150) COLLATE pg_catalog."default",
    device_status_id integer,
    status character varying(50) COLLATE pg_catalog."default",
    carrier_rate_plan_id integer,
    rate_plan character varying(255) COLLATE pg_catalog."default",
    communication_plan character varying(255) COLLATE pg_catalog."default",
    last_usage_date timestamp without time zone,
    apn character varying(250) COLLATE pg_catalog."default",
    "package" character varying(250) COLLATE pg_catalog."default",
    billing_cycle_end_date timestamp without time zone,
    bill_year integer,
    bill_month integer,
    carrier_cycle_usage bigint,
    ctd_sms_usage bigint,
    ctd_voice_usage bigint,
    ctd_session_count bigint,
    overage_limit_reached boolean,
    overage_limit_override character varying(50) COLLATE pg_catalog."default",
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    last_activated_date timestamp without time zone,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean,
    account_number character varying(50) COLLATE pg_catalog."default",
    provider_date_added timestamp without time zone,
    provider_date_activated timestamp without time zone,
    old_device_status_id integer,
    old_ctd_data_usage bigint,
    cost_center character varying(250) COLLATE pg_catalog."default",
    username character varying(250) COLLATE pg_catalog."default",
    account_number_integration_authentication_id integer,
    billing_period_id integer,
    customer_id integer,
    customer_rate_plan_id integer,
    customer_rate_pool_id integer,
    device_tenant_id integer,
    tenant_id integer,
    is_pushed boolean,
    customer_data_allocation_mb numeric(25,4)

);
CREATE INDEX IF NOT EXISTS idx_devicehistory_bill_year_bill_month
    ON public.device_history USING btree
    (bill_year ASC NULLS LAST, bill_month ASC NULLS LAST, iccid COLLATE pg_catalog."default" ASC NULLS LAST)
    INCLUDE(billing_cycle_end_date, changed_date, carrier_cycle_usage, device_status_id, status)
    TABLESPACE pg_default;
-- Index: idx_devicehistory_billing_period_id

-- DROP INDEX IF EXISTS public.idx_devicehistory_billing_period_id;

CREATE INDEX IF NOT EXISTS idx_devicehistory_billing_period_id
    ON public.device_history USING btree
    (billing_period_id ASC NULLS LAST)
    INCLUDE(billing_cycle_end_date, changed_date, carrier_cycle_usage, device_status_id, iccid, last_activated_date, service_provider_id, status)
    TABLESPACE pg_default;
-- Index: idx_devicehistory_device_status_id

-- DROP INDEX IF EXISTS public.idx_devicehistory_device_status_id;

CREATE INDEX IF NOT EXISTS idx_devicehistory_device_status_id
    ON public.device_history USING btree
    (device_status_id ASC NULLS LAST)
    INCLUDE(billing_cycle_end_date, billing_period_id, changed_date, carrier_cycle_usage, ctd_sms_usage, device_tenant_id, id, last_activated_date, service_provider_id, status)
    TABLESPACE pg_default;
-- Index: idx_devicehistory_iccid

-- DROP INDEX IF EXISTS public.idx_devicehistory_iccid;

CREATE INDEX IF NOT EXISTS idx_devicehistory_iccid
    ON public.device_history USING btree
    (iccid COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_devicehistory_id_billing_period_id

-- DROP INDEX IF EXISTS public.idx_devicehistory_id_billing_period_id;

CREATE INDEX IF NOT EXISTS idx_devicehistory_id_billing_period_id
    ON public.device_history USING btree
    (id ASC NULLS LAST, billing_period_id ASC NULLS LAST)
    INCLUDE(billing_cycle_end_date, changed_date, carrier_cycle_usage, ctd_sms_usage, device_status_id, last_activated_date, service_provider_id)
    TABLESPACE pg_default;
-- Index: idx_mobility_devicehistory_iccid

-- DROP INDEX IF EXISTS public.idx_mobility_devicehistory_iccid;

CREATE INDEX IF NOT EXISTS idx_mobility_devicehistory_iccid
    ON public.device_history USING btree
    (iccid COLLATE pg_catalog."default" ASC NULLS LAST);

CREATE TABLE IF NOT EXISTS public.mobility_device_history
(
    device_history_id serial primary key,
    changed_date timestamp without time zone NOT NULL,
    id integer NOT NULL,
    service_provider_id integer NOT NULL,
    foundation_account_number character varying(30) COLLATE pg_catalog."default" NOT NULL,
    billing_account_number character varying(30) COLLATE pg_catalog."default" NOT NULL,
    iccid character varying(50) COLLATE pg_catalog."default" NOT NULL,
    imsi character varying(150) COLLATE pg_catalog."default",
    msisdn character varying(150) COLLATE pg_catalog."default",
    imei character varying(150) COLLATE pg_catalog."default",
    device_status_id integer,
    status character varying(50) COLLATE pg_catalog."default",
    carrier_rate_plan_id integer,
    rate_plan character varying(200) COLLATE pg_catalog."default",
    last_usage_date timestamp without time zone,
    carrier_cycle_usage bigint,
    ctd_sms_usage bigint,
    ctd_voice_usage bigint,
    ctd_session_count bigint,
    created_by character varying(100) COLLATE pg_catalog."default" NOT NULL,
    created_date timestamp without time zone,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone,
    last_activated_date timestamp without time zone,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean NOT NULL,
    account_number character varying(50) COLLATE pg_catalog."default",
    provider_date_added timestamp without time zone,
    provider_date_activated timestamp without time zone,
    old_device_status_id integer,
    old_ctd_data_usage bigint,
    account_number_integration_authentication_id integer,
    billing_period_id integer,
    customer_id integer,
    single_user_code character varying(200) COLLATE pg_catalog."default",
    single_user_code_description character varying(200) COLLATE pg_catalog."default",
    service_zip_code character varying(50) COLLATE pg_catalog."default",
    data_group_id character varying(50) COLLATE pg_catalog."default",
    pool_id character varying(50) COLLATE pg_catalog."default",
    device_make character varying(50) COLLATE pg_catalog."default",
    device_model character varying(50) COLLATE pg_catalog."default",
    contract_status character varying(50) COLLATE pg_catalog."default",
    ban_status character varying(50) COLLATE pg_catalog."default",
    imei_type_id integer,
    plan_limit_mb numeric(25,4),
    customer_rate_plan_id integer,
    customer_data_allocation_mb numeric(25,4),
    username character varying(150) COLLATE pg_catalog."default",
    customer_rate_pool_id integer,
    mobility_device_tenant_id integer,
    tenant_id integer,
    ip_address character varying(50) COLLATE pg_catalog."default",
    is_pushed boolean NOT NULL
);

CREATE TABLE IF NOT EXISTS public.sim_management_inventory_history
(
    device_history_id serial primary key,
    id integer,
    changed_date timestamp without time zone NOT NULL,
    d_device_history_id integer,
    m_device_history_id integer,
    service_provider_id integer NOT NULL,
    iccid character varying(50) COLLATE pg_catalog."default" NOT NULL,
    imsi character varying(150) COLLATE pg_catalog."default",
    msisdn character varying(150) COLLATE pg_catalog."default",
    imei character varying(150) COLLATE pg_catalog."default",
    device_status_id integer,
    status character varying(50) COLLATE pg_catalog."default",
    carrier_rate_plan_id integer,
    rate_plan character varying(255) COLLATE pg_catalog."default",
    last_usage_date timestamp without time zone,
    apn character varying(250) COLLATE pg_catalog."default",
    carrier_cycle_usage bigint,
    ctd_sms_usage bigint,
    ctd_voice_usage bigint,
    ctd_session_count bigint,
    created_by character varying(100) COLLATE pg_catalog."default" NOT NULL,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    last_activated_date timestamp without time zone,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean NOT NULL,
    account_number character varying(50) COLLATE pg_catalog."default",
    provider_date_added timestamp without time zone,
    provider_date_activated timestamp without time zone,
    old_device_status_id integer,
    old_ctd_data_usage bigint,
    account_number_integration_authentication_id integer,
    billing_period_id integer,
    customer_id integer,
    single_user_code character varying(200) COLLATE pg_catalog."default",
    single_user_code_description character varying(200) COLLATE pg_catalog."default",
    service_zip_code character varying(50) COLLATE pg_catalog."default",
    data_group_id character varying(50) COLLATE pg_catalog."default",
    pool_id character varying(50) COLLATE pg_catalog."default",
    device_make character varying(50) COLLATE pg_catalog."default",
    device_model character varying(50) COLLATE pg_catalog."default",
    contract_status character varying(50) COLLATE pg_catalog."default",
    ban_status character varying(50) COLLATE pg_catalog."default",
    imei_type_id integer,
    plan_limit_mb numeric(25,4),
    customer_rate_pool_id integer,
    mobility_device_tenant_id integer,
    tenant_id integer,
    "package" character varying(250) COLLATE pg_catalog."default",
    ip_address character varying(50) COLLATE pg_catalog."default",
    is_pushed boolean NOT NULL,
    billing_account_number character varying(30) COLLATE pg_catalog."default",
    foundation_account_number character varying(30) COLLATE pg_catalog."default",
    customer_rate_plan_id integer,
    customer_data_allocation_mb numeric(25,4),
    username character varying(150) COLLATE pg_catalog."default",
    billing_cycle_end_date timestamp without time zone,
    bill_year integer,
    bill_month integer,
    overage_limit_reached boolean,
    overage_limit_override character varying(50) COLLATE pg_catalog."default",
    cost_center character varying(250) COLLATE pg_catalog."default",
    device_tenant_id integer,
    smi_id bigint,
    communication_plan varchar null
    CONSTRAINT fk_sim_management_inventory_id FOREIGN KEY (smi_id)
        REFERENCES public.sim_management_inventory (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS public.device_status_history
(
    id serial primary key,
    sim_management_inventory_id integer,
    iccid character varying(50) COLLATE pg_catalog."default",
    msisdn character varying(50) COLLATE pg_catalog."default",
    username character varying(150) COLLATE pg_catalog."default",
    previous_status character varying(50) COLLATE pg_catalog."default",
    current_status character varying(50) COLLATE pg_catalog."default",
    change_event_type character varying(250) COLLATE pg_catalog."default",
    date_of_change timestamp without time zone,
    changed_by character varying(100) COLLATE pg_catalog."default",
    is_active boolean default true,
    service_provider_id integer,
    device_id integer,
    mobility_device_id integer,
    uploaded_file_id integer,
    bulk_change_id bigint,
    customer_name character varying(500) COLLATE pg_catalog."default",
    customer_account_number character varying(50) COLLATE pg_catalog."default",
    customer_rate_plan character varying(50) COLLATE pg_catalog."default",
    customer_rate_pool character varying(200) COLLATE pg_catalog."default",
    tenant_id integer,
    CONSTRAINT fk_bulk_change_id FOREIGN KEY (bulk_change_id)
        REFERENCES public.sim_management_bulk_change (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL,
    CONSTRAINT fk_service_provider_id FOREIGN KEY (service_provider_id)
        REFERENCES public.serviceprovider (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL,
    CONSTRAINT fk_sim_management_inventory_id FOREIGN KEY (sim_management_inventory_id)
        REFERENCES public.sim_management_inventory (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_device_status_history_current_status
    ON public.device_status_history USING btree
    (current_status COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_device_status_history_date_and_status

-- DROP INDEX IF EXISTS public.idx_device_status_history_date_and_status;

CREATE INDEX IF NOT EXISTS idx_device_status_history_date_and_status
    ON public.device_status_history USING btree
    (mobility_device_id ASC NULLS LAST, date_of_change DESC NULLS FIRST, current_status COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_device_status_history_date_of_change

-- DROP INDEX IF EXISTS public.idx_device_status_history_date_of_change;

CREATE INDEX IF NOT EXISTS idx_device_status_history_date_of_change
    ON public.device_status_history USING btree
    (date_of_change ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_device_status_history_date_of_change_desc

-- DROP INDEX IF EXISTS public.idx_device_status_history_date_of_change_desc;

CREATE INDEX IF NOT EXISTS idx_device_status_history_date_of_change_desc
    ON public.device_status_history USING btree
    (mobility_device_id ASC NULLS LAST, date_of_change DESC NULLS FIRST)
    TABLESPACE pg_default;
-- Index: idx_device_status_history_device_id

-- DROP INDEX IF EXISTS public.idx_device_status_history_device_id;

CREATE INDEX IF NOT EXISTS idx_device_status_history_device_id
    ON public.device_status_history USING btree
    (device_id ASC NULLS LAST)
    INCLUDE(bulk_change_id, changed_by, change_event_type, current_status, customer_account_number, customer_name, customer_rate_plan, customer_rate_pool, date_of_change, iccid, msisdn, previous_status, service_provider_id, username)
    TABLESPACE pg_default;
-- Index: idx_device_status_history_device_tenant_date

-- DROP INDEX IF EXISTS public.idx_device_status_history_device_tenant_date;

CREATE INDEX IF NOT EXISTS idx_device_status_history_device_tenant_date
    ON public.device_status_history USING btree
    (device_id ASC NULLS LAST, tenant_id ASC NULLS LAST, date_of_change DESC NULLS FIRST)
    INCLUDE(current_status)
    TABLESPACE pg_default;
-- Index: idx_device_status_history_iccid

-- DROP INDEX IF EXISTS public.idx_device_status_history_iccid;

CREATE INDEX IF NOT EXISTS idx_device_status_history_iccid
    ON public.device_status_history USING btree
    (iccid COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_device_status_history_iccid_date_of_change

-- DROP INDEX IF EXISTS public.idx_device_status_history_iccid_date_of_change;

CREATE INDEX IF NOT EXISTS idx_device_status_history_iccid_date_of_change
    ON public.device_status_history USING btree
    (iccid COLLATE pg_catalog."default" ASC NULLS LAST, date_of_change ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_device_status_history_iccid_status

-- DROP INDEX IF EXISTS public.idx_device_status_history_iccid_status;

CREATE INDEX IF NOT EXISTS idx_device_status_history_iccid_status
    ON public.device_status_history USING btree
    (iccid COLLATE pg_catalog."default" ASC NULLS LAST, current_status COLLATE pg_catalog."default" ASC NULLS LAST, date_of_change DESC NULLS FIRST)
    TABLESPACE pg_default;
-- Index: idx_device_status_history_lower_current_status

-- DROP INDEX IF EXISTS public.idx_device_status_history_lower_current_status;

CREATE INDEX IF NOT EXISTS idx_device_status_history_lower_current_status
    ON public.device_status_history USING btree
    (lower(current_status::text) COLLATE pg_catalog."default" ASC NULLS LAST, lower(previous_status::text) COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_device_status_history_mobility_device

-- DROP INDEX IF EXISTS public.idx_device_status_history_mobility_device;

CREATE INDEX IF NOT EXISTS idx_device_status_history_mobility_device
    ON public.device_status_history USING btree
    (mobility_device_id ASC NULLS LAST)
    INCLUDE(bulk_change_id, changed_by, change_event_type, current_status, customer_account_number, customer_name, customer_rate_plan, customer_rate_pool, date_of_change, iccid, msisdn, previous_status, service_provider_id, username)
    TABLESPACE pg_default;
-- Index: idx_device_status_history_msisdn_deleted_date

-- DROP INDEX IF EXISTS public.idx_device_status_history_msisdn_deleted_date;

CREATE INDEX IF NOT EXISTS idx_device_status_history_msisdn_deleted_date
    ON public.device_status_history USING btree
    (msisdn COLLATE pg_catalog."default" ASC NULLS LAST, date_of_change DESC NULLS FIRST)
    INCLUDE(current_status, previous_status)
    TABLESPACE pg_default;
-- Index: idx_device_status_history_previous_status

-- DROP INDEX IF EXISTS public.idx_device_status_history_previous_status;

CREATE INDEX IF NOT EXISTS idx_device_status_history_previous_status
    ON public.device_status_history USING btree
    (previous_status COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_device_status_history_tenant

-- DROP INDEX IF EXISTS public.idx_device_status_history_tenant;

CREATE INDEX IF NOT EXISTS idx_device_status_history_tenant
    ON public.device_status_history USING btree
    (tenant_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_device_status_history_tenant_iccid_sim_inventory

-- DROP INDEX IF EXISTS public.idx_device_status_history_tenant_iccid_sim_inventory;

CREATE INDEX IF NOT EXISTS idx_device_status_history_tenant_iccid_sim_inventory
    ON public.device_status_history USING btree
    (tenant_id ASC NULLS LAST, iccid COLLATE pg_catalog."default" ASC NULLS LAST, sim_management_inventory_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_device_status_history_tenant_null

-- DROP INDEX IF EXISTS public.idx_device_status_history_tenant_null;

CREATE INDEX IF NOT EXISTS idx_device_status_history_tenant_null
    ON public.device_status_history USING btree
    (device_id ASC NULLS LAST)
    TABLESPACE pg_default
    WHERE tenant_id IS NULL;
-- Index: idx_service_provider_id

-- DROP INDEX IF EXISTS public.idx_service_provider_id;

CREATE INDEX IF NOT EXISTS idx_service_provider_id
    ON public.device_status_history USING btree
    (service_provider_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: ix_device_status_history_mobility_device_id_tenant_id_date_of_c

-- DROP INDEX IF EXISTS public.ix_device_status_history_mobility_device_id_tenant_id_date_of_c;

CREATE INDEX IF NOT EXISTS ix_device_status_history_mobility_device_id_tenant_id_date_of_c
    ON public.device_status_history USING btree
    (mobility_device_id ASC NULLS LAST, tenant_id ASC NULLS LAST, date_of_change DESC NULLS FIRST)
    INCLUDE(current_status)
    TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS public.device_status_reason_code
(
    id serial primary key,
    device_status_id integer,
    device_status character varying(255) COLLATE pg_catalog."default",
    reason_code character varying(50) COLLATE pg_catalog."default",
    description character varying(100) COLLATE pg_catalog."default",
    service_provider_id integer,
    service_provider character varying(100) COLLATE pg_catalog."default",
    tenant_id integer,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean
);

CREATE TABLE IF NOT EXISTS public.device_usage
(
    id serial primary key,
    sim_management_inventory_id integer,
    m2m_device_id integer,
    mobility_device_id integer,
    data_usage bigint,
    sms_usage bigint,
    voice_usage bigint,
    usage_date timestamp without time zone,
    device_status_id integer,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone,
    CONSTRAINT fk_sim_management_inventory_id FOREIGN KEY (sim_management_inventory_id)
        REFERENCES public.sim_management_inventory (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_device_usage_customer_bill_period
    ON public.device_usage USING btree
    (data_usage ASC NULLS LAST, usage_date ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_device_usage_m2m_device_id

-- DROP INDEX IF EXISTS public.idx_device_usage_m2m_device_id;

CREATE INDEX IF NOT EXISTS idx_device_usage_m2m_device_id
    ON public.device_usage USING btree
    (m2m_device_id ASC NULLS LAST)
    INCLUDE(data_usage, usage_date)
    TABLESPACE pg_default;
-- Index: idx_device_usage_mobility_device_id

-- DROP INDEX IF EXISTS public.idx_device_usage_mobility_device_id;

CREATE INDEX IF NOT EXISTS idx_device_usage_mobility_device_id
    ON public.device_usage USING btree
    (mobility_device_id ASC NULLS LAST)
    INCLUDE(data_usage, usage_date)
    TABLESPACE pg_default;
-- Index: idx_du_mobility_device_id

-- DROP INDEX IF EXISTS public.idx_du_mobility_device_id;

CREATE INDEX IF NOT EXISTS idx_du_mobility_device_id
    ON public.device_usage USING btree
    (mobility_device_id ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS public.netsapiens_device
(
    id serial primary key,
    subscriber_domain character varying(256) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    subscriber_name character varying(256) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    aor character varying(256) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    mode character varying(256) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    user_agent character varying(512) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    received_from character varying(128) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    contact character varying(128) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    registration_time timestamp without time zone,
    authentication_key character varying(50) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    call_processing_rule character varying(50) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    registration_expires_time timestamp without time zone,
    expires character varying(50) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    callid_emgr character varying(128) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    sub_fullname character varying(128) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    sub_login character varying(128) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    aor_user character varying(50) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    sub_scope character varying(128) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    nd_perror character varying(3000) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    server character varying(1024) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    auth_user character varying(1024) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    auth_pass character varying(1024) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    mac character varying(128) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    model character varying(128) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    transport character varying(50) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    line character varying(50) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    created_by character varying(100) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default" DEFAULT NULL::character varying,
    deleted_date timestamp without time zone,
    is_active boolean,
    can_ignore boolean
);

CREATE TABLE IF NOT EXISTS public.optimization_session
(
    id bigserial primary key,
    session_id uuid,
    billing_period_start_date timestamp without time zone,
    billing_period_end_date timestamp without time zone,
    tenant_id integer,
    service_provider_id integer,
    service_provider_ids text COLLATE pg_catalog."default",
    customer_id integer,
    optimization_type_id bigint,
    integration_id integer,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean,
    progress varchar null,
    optimization_run_time_error varchar(100) null,
    CONSTRAINT fk_optimizationsession_integration FOREIGN KEY (integration_id)
        REFERENCES public.integration (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_optimizationsession_optimizationtype FOREIGN KEY (optimization_type_id)
        REFERENCES public.optimization_type (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_optimizationsession_serviceprovider FOREIGN KEY (service_provider_id)
        REFERENCES public.serviceprovider (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE IF NOT EXISTS public.optimization_instance
(
    id bigserial primary key,
    billing_period_start_date timestamp without time zone,
    billing_period_end_date timestamp without time zone,
    run_status_id integer,
    run_start_time timestamp without time zone,
    run_end_time timestamp without time zone,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean,
    sqs_message_id character varying(50) COLLATE pg_catalog."default",
    rev_customer_id uuid,
    service_provider_id integer,
    integration_authentication_id integer,
    tenant_id integer,
    portal_type_id integer,
    row_uuid character varying(50) COLLATE pg_catalog."default",
    integration_id integer,
    optimization_session_id bigint,
    optimization_billing_period_id integer,
    use_bill_in_advance boolean,
    bill_in_advance_billing_period_id integer,
    amop_customer_id integer,
    customer_billing_period_id integer,
    customer_bill_in_advance_billing_period_id integer,
    service_provider_ids text COLLATE pg_catalog."default",
    CONSTRAINT fk_customers_amopcustomerid FOREIGN KEY (amop_customer_id)
        REFERENCES public.customers (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_optimizationinstance_billinadvancebillingperiod FOREIGN KEY (bill_in_advance_billing_period_id)
        REFERENCES public.billing_period (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_optimizationinstance_optimizationbillingperiod FOREIGN KEY (optimization_billing_period_id)
        REFERENCES public.billing_period (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_optimizationinstance_optimizationsession FOREIGN KEY (optimization_session_id)
        REFERENCES public.optimization_session (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_optimizationinstance_optimizationstatus FOREIGN KEY (run_status_id)
        REFERENCES public.optimization_status (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_optimizationinstance_serviceprovider FOREIGN KEY (service_provider_id)
        REFERENCES public.serviceprovider (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);
CREATE INDEX IF NOT EXISTS idx_optimization_instance_created_date
    ON public.optimization_instance USING btree
    (created_date ASC NULLS LAST);

CREATE TABLE IF NOT EXISTS public.optimization_comm_group
(
    id bigserial primary key,
    instance_id bigint,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean default true,
    row_uuid character varying(50) COLLATE pg_catalog."default",
    CONSTRAINT fk_optimizationcommgroup_optimizationinstance FOREIGN KEY (instance_id)
        REFERENCES public.optimization_instance (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE IF NOT EXISTS public.optimization_group
(
    id serial primary key,
    optimization_group_name character varying(250) COLLATE pg_catalog."default",
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean,
    service_provider_id integer,
    service_provider_name character varying(100) COLLATE pg_catalog."default",
    alias_name character varying(250) COLLATE pg_catalog."default",
    tenant_id integer,
    rate_plans_list text COLLATE pg_catalog."default",
    CONSTRAINT unique_id_optimization_group UNIQUE (id, optimization_group_name),
    CONSTRAINT fk_optimizationgroup_serviceprovider FOREIGN KEY (service_provider_id)
        REFERENCES public.serviceprovider (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE IF NOT EXISTS public.optimization_customer_processing
(
    id bigserial primary key,
    service_provider_id integer,
    service_provider character varying(100) COLLATE pg_catalog."default",
    customer_id integer,
    customer_name character varying(255) COLLATE pg_catalog."default",
    amop_customer_id integer,
    amop_customer_name character varying(255) COLLATE pg_catalog."default",
    device_count integer,
    is_processed boolean,
    start_time timestamp without time zone,
    end_time timestamp without time zone,
    instance_id bigint,
    session_id bigint,
    error_message text COLLATE pg_catalog."default",
    tenant_id integer
);

CREATE TABLE IF NOT EXISTS public.optimization_group_carrier_rate_plan
(
    id serial primary key,
    optimization_group_id integer,
    optimization_group_name character varying(80) COLLATE pg_catalog."default",
    rate_plan_id integer,
    rate_plan_code character varying(100) COLLATE pg_catalog."default",
    created_by character varying(100) COLLATE pg_catalog."default" NOT NULL,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean,
    tenant_id integer,
    CONSTRAINT fk_optimizationgroup_carrierrateplan_jaspercarrierrateplan FOREIGN KEY (rate_plan_id)
        REFERENCES public.carrier_rate_plan (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_optimizationgroup_carrierrateplan_optimizationgroup FOREIGN KEY (optimization_group_id)
        REFERENCES public.optimization_group (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE IF NOT EXISTS public.optimization_instance_result_file
(
    id bigserial primary key,
    instance_id bigint,
    stat_file_bytes bytea,
    assignment_file_bytes bytea,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean DEFAULT false,
    assignment_xlsx_bytes bytea,
    CONSTRAINT fk_optimizationinstance FOREIGN KEY (instance_id)
        REFERENCES public.optimization_instance (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL
);

CREATE TABLE IF NOT EXISTS public.optimization_queue
(
    id bigserial primary key,
    instance_id bigint,
    comm_plan_group_id bigint,
    run_status_id integer NOT NULL,
    run_start_time timestamp without time zone,
    run_end_time timestamp without time zone,
    total_cost numeric(25,4),
    sqs_message_id character varying(50) COLLATE pg_catalog."default",
    created_by character varying(100) COLLATE pg_catalog."default" DEFAULT 'System'::character varying,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default" DEFAULT CURRENT_TIMESTAMP,
    modified_date timestamp without time zone,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean DEFAULT false,
    service_provider_id integer,
    row_uuid uuid DEFAULT gen_random_uuid(),
    uses_proration boolean DEFAULT false,
    is_bill_in_advance boolean DEFAULT false,
    total_base_rate_amt numeric(25,4),
    total_rate_charge_amt numeric(25,4),
    total_overage_charge_amt numeric(25,4),
    CONSTRAINT fk_optimizationqueue_optimizationcommgroup FOREIGN KEY (comm_plan_group_id)
        REFERENCES public.optimization_comm_group (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_optimizationqueue_optimizationinstance FOREIGN KEY (instance_id)
        REFERENCES public.optimization_instance (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_optimizationqueue_optimizationstatus FOREIGN KEY (run_status_id)
        REFERENCES public.optimization_status (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);
CREATE INDEX IF NOT EXISTS idx_optimization_queue_instance_id
    ON public.optimization_queue USING btree
    (instance_id ASC NULLS LAST);
CREATE INDEX IF NOT EXISTS ix_optimization_queue_comm_plan_group_id
    ON public.optimization_queue USING btree
    (comm_plan_group_id ASC NULLS LAST);
CREATE INDEX IF NOT EXISTS ix_optimization_queue_run_end_time
    ON public.optimization_queue USING btree
    (run_end_time ASC NULLS LAST);
CREATE INDEX IF NOT EXISTS ix_optimization_queue_total_cost
    ON public.optimization_queue USING btree
    (total_cost ASC NULLS LAST)
    INCLUDE(comm_plan_group_id, total_base_rate_amt, total_overage_charge_amt, total_rate_charge_amt);

CREATE TABLE IF NOT EXISTS public.optimization_rate_plan_type
(
    id serial primary key,
    rate_plan_type_name character varying(250) COLLATE pg_catalog."default",
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean,
    service_provider_id integer,
    service_provider_name character varying(100) COLLATE pg_catalog."default",
    tenant_id integer,
    CONSTRAINT fk_optimizationrateplantype_serviceprovider FOREIGN KEY (service_provider_id)
        REFERENCES public.serviceprovider (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE IF NOT EXISTS public.optimization_device
(
    id bigserial primary key,
    instance_id bigint,
    device_id integer,
    cycle_data_usage_mb numeric(25,4),
    projected_data_usage_mb numeric(25,4),
    communication_plan character varying(50) COLLATE pg_catalog."default",
    msisdn character varying(150) COLLATE pg_catalog."default",
    iccid character varying(50) COLLATE pg_catalog."default",
    usage_date timestamp without time zone,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    amop_device_id integer,
    service_provider_id integer,
    date_activated timestamp without time zone,
    was_activated_in_this_billing_period boolean,
    days_activated_in_billing_period integer,
    sms_usage bigint,
    auto_change_rate_plan boolean,
    optimization_comm_group_id bigint
);

CREATE TABLE IF NOT EXISTS public.optimization_mobility_device
(
    id bigserial primary key,
    instance_id bigint,
    device_id integer,
    cycle_data_usage_mb numeric(25,4),
    projected_data_usage_mb numeric(25,4),
    communication_plan character varying(50) COLLATE pg_catalog."default",
    msisdn character varying(150) COLLATE pg_catalog."default",
    iccid character varying(50) COLLATE pg_catalog."default",
    usage_date timestamp without time zone,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone,
    amop_device_id integer,
    service_provider_id integer,
    date_activated timestamp without time zone,
    was_activated_in_this_billing_period boolean,
    days_activated_in_billing_period integer,
    sms_usage bigint,
    optimization_rate_plan_type_id integer,
    optimization_group_id integer,
    auto_change_rate_plan boolean,
    optimization_comm_group_id bigint
);

CREATE TABLE IF NOT EXISTS public.optimization_smi
(
    id bigserial primary key,
    instance_id bigint,
    device_id integer,
    cycle_data_usage_mb numeric(25,4),
    projected_data_usage_mb numeric(25,4),
    communication_plan character varying(50) COLLATE pg_catalog."default",
    msisdn character varying(150) COLLATE pg_catalog."default",
    iccid character varying(50) COLLATE pg_catalog."default",
    usage_date timestamp without time zone,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone,
    amop_device_id integer,
    service_provider_id integer,
    date_activated timestamp without time zone,
    was_activated_in_this_billing_period boolean,
    days_activated_in_billing_period integer,
    sms_usage bigint,
    optimization_comm_group_id bigint,
    optimization_rate_plan_type_id integer,
    optimization_group_id integer,
    auto_change_rate_plan boolean,
    did bigint,
    mid bigint,
    sim_management_inventory_id bigint,
    CONSTRAINT fk_optimizationgroupname FOREIGN KEY (optimization_group_id)
        REFERENCES public.optimization_group (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL,
    CONSTRAINT fk_optimizationinstance FOREIGN KEY (instance_id)
        REFERENCES public.optimization_instance (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL,
    CONSTRAINT fk_optimizationrateplantype FOREIGN KEY (optimization_rate_plan_type_id)
        REFERENCES public.optimization_rate_plan_type (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL,
    CONSTRAINT fk_sim_management_inventory_id FOREIGN KEY (sim_management_inventory_id)
        REFERENCES public.sim_management_inventory (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_optimization_device_instance_id
    ON public.optimization_smi USING btree
    (instance_id ASC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_optimization_device_instance_id_service_provider_id
    ON public.optimization_smi USING btree
    (instance_id ASC NULLS LAST, service_provider_id ASC NULLS LAST)
    INCLUDE(amop_device_id, communication_plan, created_by, created_date, cycle_data_usage_mb, date_activated, iccid, msisdn, projected_data_usage_mb, sms_usage, usage_date);

CREATE TABLE IF NOT EXISTS public.optimization_device_result
(
    id bigserial primary key,
    queue_id bigint,
    device_id integer,
    usage_mb numeric(25,4),
    assigned_carrier_rate_plan_id integer,
    assigned_customer_rate_plan_id integer,
    customer_rate_pool_id integer,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    amop_device_id integer,
    charge_amt numeric(25,4),
    billing_period_id integer,
    sms_usage bigint,
    sms_charge_amount numeric(25,4),
    base_rate_amt numeric(25,4),
    rate_charge_amt numeric(25,4),
    overage_charge_amt numeric(25,4)
);

CREATE TABLE IF NOT EXISTS public.optimization_mobility_device_result
(
    id bigserial primary key,
    queue_id bigint,
    device_id integer,
    usage_mb numeric(25,4),
    assigned_carrier_rate_plan_id integer,
    assigned_customer_rate_plan_id integer,
    customer_rate_pool_id integer,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    amop_device_id integer,
    charge_amt numeric(25,4),
    billing_period_id integer,
    sms_usage bigint,
    sms_charge_amount numeric(25,4),
    base_rate_amt numeric(25,4),
    rate_charge_amt numeric(25,4),
    overage_charge_amt numeric(25,4)
);

CREATE TABLE IF NOT EXISTS public.optimization_smi_result
(
    id bigserial primary key,
    queue_id bigint,
    device_id integer,
    usage_mb numeric(25,4),
    assigned_carrier_rate_plan_id integer,
    assigned_customer_rate_plan_id integer,
    customer_rate_pool_id integer,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    amop_device_id integer,
    charge_amt numeric(25,4),
    billing_period_id integer,
    sms_usage bigint,
    sms_charge_amount numeric(25,4),
    base_rate_amt numeric(25,4),
    rate_charge_amt numeric(25,4),
    overage_charge_amt numeric(25,4),
    did bigint,
    mid bigint,
    sim_management_inventory_id bigint,
    CONSTRAINT fk_carrierrateplan FOREIGN KEY (assigned_carrier_rate_plan_id)
        REFERENCES public.carrier_rate_plan (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_customerrateplan FOREIGN KEY (assigned_customer_rate_plan_id)
        REFERENCES public.customerrateplan (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_customerratepool FOREIGN KEY (customer_rate_pool_id)
        REFERENCES public.customer_rate_pool (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_sim_management_inventory_id FOREIGN KEY (sim_management_inventory_id)
        REFERENCES public.sim_management_inventory (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE SET NULL
);
CREATE INDEX IF NOT EXISTS idx_optimization_smi_result_queue_id
    ON public.optimization_smi_result USING btree
    (queue_id ASC NULLS LAST)
    INCLUDE(amop_device_id, assigned_carrier_rate_plan_id, assigned_customer_rate_plan_id, charge_amt, id, sms_charge_amount, usage_mb)
    TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS public.optimization_device_result_customer_charge_queue
(
    id bigserial primary key,
    optimization_device_result_id bigint,
    is_processed boolean,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone,
    charge_amount numeric(25,4),
    charge_id integer,
    base_charge_amount numeric(25,4),
    total_charge_amount numeric(25,4),
    has_errors boolean,
    error_message character varying(1000) COLLATE pg_catalog."default",
    rev_service_number character varying(250) COLLATE pg_catalog."default",
    rev_product_type_id integer,
    uploaded_file_id integer,
    billing_start_date timestamp without time zone,
    billing_end_date timestamp without time zone,
    description character varying(250) COLLATE pg_catalog."default",
    integration_authentication_id integer,
    billing_period_id integer,
    sms_rev_product_type_id integer,
    sms_charge_amount numeric(25,4),
    sms_charge_id integer,
    rate_charge_amt numeric(25,4),
    overage_charge_amt numeric(25,4),
    base_rate_amt numeric(25,4),
    overage_rev_product_type_id integer,
    rev_product_id integer,
    sms_rev_product_id integer,
    overage_rev_product_id integer
);

CREATE TABLE IF NOT EXISTS public.optimization_mobility_device_result_customer_charge_queue
(
    id bigserial primary key,
    optimization_mobility_device_result_id bigint,
    is_processed boolean,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone,
    charge_amount numeric(25,4),
    charge_id integer,
    base_charge_amount numeric(25,4),
    total_charge_amount numeric(25,4),
    has_errors boolean,
    error_message character varying(1000) COLLATE pg_catalog."default",
    rev_service_number character varying(250) COLLATE pg_catalog."default",
    rev_product_type_id integer,
    uploaded_file_id integer,
    billing_start_date timestamp without time zone,
    billing_end_date timestamp without time zone,
    description character varying(250) COLLATE pg_catalog."default",
    integration_authentication_id integer,
    billing_period_id integer,
    sms_rev_product_type_id integer,
    sms_charge_amount numeric(25,4),
    sms_charge_id integer,
    rate_charge_amt numeric(25,4),
    overage_charge_amt numeric(25,4),
    base_rate_amt numeric(25,4),
    overage_rev_product_type_id integer,
    rev_product_id integer,
    sms_rev_product_id integer,
    overage_rev_product_id integer
);

CREATE TABLE IF NOT EXISTS public.optimization_smi_result_customer_charge_queue
(
    id bigserial primary key,
    optimization_device_result_id bigint,
    optimization_mobility_device_result_id bigint,
    is_processed boolean,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone,
    charge_amount numeric(25,4),
    charge_id integer,
    base_charge_amount numeric(25,4),
    total_charge_amount numeric(25,4),
    has_errors boolean,
    error_message character varying(1000) COLLATE pg_catalog."default",
    rev_service_number character varying(250) COLLATE pg_catalog."default",
    rev_product_type_id integer,
    uploaded_file_id integer,
    billing_start_date timestamp without time zone,
    billing_end_date timestamp without time zone,
    description character varying(250) COLLATE pg_catalog."default",
    integration_authentication_id integer,
    billing_period_id integer,
    sms_rev_product_type_id integer,
    sms_charge_amount numeric(25,4),
    sms_charge_id integer,
    rate_charge_amt numeric(25,4),
    overage_charge_amt numeric(25,4),
    base_rate_amt numeric(25,4),
    overage_rev_product_type_id integer,
    rev_product_id integer,
    sms_rev_product_id integer,
    overage_rev_product_id integer,
    did bigint,
    mid bigint,
    optimization_smi_result_id bigint,
    CONSTRAINT fk_billingperiod FOREIGN KEY (billing_period_id)
        REFERENCES public.billing_period (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_optimizationsmiresult FOREIGN KEY (optimization_smi_result_id)
        REFERENCES public.optimization_smi_result (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE IF NOT EXISTS public.optimization_device_result_rate_plan_queue
(
    id bigserial primary key,
    optimization_device_result_id bigint NOT NULL,
    is_processed boolean NOT NULL,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone,
    group_number integer NOT NULL,
    has_errors boolean NOT NULL,
    error_message text COLLATE pg_catalog."default"
);

CREATE TABLE IF NOT EXISTS public.optimization_mobility_device_result_rate_plan_queue
(
    id bigserial primary key,
    optimization_mobility_device_result_id bigint NOT NULL,
    is_processed boolean NOT NULL,
    created_by character varying(100) COLLATE pg_catalog."default" NOT NULL,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    group_number integer NOT NULL,
    has_errors boolean NOT NULL,
    error_message text COLLATE pg_catalog."default"
);

CREATE TABLE IF NOT EXISTS public.optimization_smi_result_rate_plan_queue
(
    id bigserial primary key,
    mid integer,
    did integer,
    optimization_mobility_device_result_id bigint,
    is_processed boolean,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    group_number integer,
    has_errors boolean,
    error_message text COLLATE pg_catalog."default",
    optimization_device_result_id bigint,
    optimization_smi_result_id bigint,
    CONSTRAINT fk_optimizationsmiresult FOREIGN KEY (optimization_smi_result_id)
        REFERENCES public.optimization_smi_result (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE IF NOT EXISTS public.rev_service_product
(
    id serial primary key,
    service_product_id integer,
    customer_id character varying(50) COLLATE pg_catalog."default",
    product_id integer,
    package_id integer,
    service_id integer,
    description character varying(1024) COLLATE pg_catalog."default",
    code1 character varying(1024) COLLATE pg_catalog."default",
    code2 character varying(1024) COLLATE pg_catalog."default",
    rate numeric(25,4),
    billed_through_date timestamp without time zone,
    canceled_date timestamp without time zone,
    status character varying(150) COLLATE pg_catalog."default",
    status_date timestamp without time zone,
    status_user_id integer,
    activated_date timestamp without time zone,
    cost numeric(25,4),
    wholesale_description character varying(512) COLLATE pg_catalog."default",
    free_start_date timestamp without time zone,
    free_end_date timestamp without time zone,
    quantity integer,
    contract_start_date timestamp without time zone,
    contract_end_date timestamp without time zone,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    tax_included boolean,
    group_on_bill boolean,
    itemized boolean,
    created_by character varying(100) COLLATE pg_catalog."default",
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean,
    integration_authentication_id integer,
    pro_rate boolean,
    CONSTRAINT fk_integration_authentication FOREIGN KEY (integration_authentication_id)
        REFERENCES public.integration_authentication (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);
CREATE INDEX IF NOT EXISTS idx_rev_service_product_active
    ON public.rev_service_product USING btree
    (service_id ASC NULLS LAST)
    TABLESPACE pg_default
    WHERE status::text = 'ACTIVE'::text;
-- Index: idx_rev_service_product_customer_id

-- DROP INDEX IF EXISTS public.idx_rev_service_product_customer_id;

CREATE INDEX IF NOT EXISTS idx_rev_service_product_customer_id
    ON public.rev_service_product USING btree
    (customer_id COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_rev_service_product_integration_authentication_id

-- DROP INDEX IF EXISTS public.idx_rev_service_product_integration_authentication_id;

CREATE INDEX IF NOT EXISTS idx_rev_service_product_integration_authentication_id
    ON public.rev_service_product USING btree
    (integration_authentication_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_rev_service_product_product_id

-- DROP INDEX IF EXISTS public.idx_rev_service_product_product_id;

CREATE INDEX IF NOT EXISTS idx_rev_service_product_product_id
    ON public.rev_service_product USING btree
    (product_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_rev_service_product_product_id_service_id

-- DROP INDEX IF EXISTS public.idx_rev_service_product_product_id_service_id;

CREATE INDEX IF NOT EXISTS idx_rev_service_product_product_id_service_id
    ON public.rev_service_product USING btree
    (product_id ASC NULLS LAST, service_id ASC NULLS LAST)
    TABLESPACE pg_default
    WHERE status::text = 'ACTIVE'::text AND is_active = true;
-- Index: idx_rev_service_product_service_id

-- DROP INDEX IF EXISTS public.idx_rev_service_product_service_id;

CREATE INDEX IF NOT EXISTS idx_rev_service_product_service_id
    ON public.rev_service_product USING btree
    (service_id ASC NULLS LAST)
    TABLESPACE pg_default;


CREATE INDEX IF NOT EXISTS idx_rev_service_product_service_id_status_is_active
    ON public.rev_service_product USING btree
    (service_id ASC NULLS LAST, status COLLATE pg_catalog."default" ASC NULLS LAST, is_active ASC NULLS LAST)
    TABLESPACE pg_default
    WHERE status::text = 'ACTIVE'::text AND is_active = true;
-- Index: idx_rev_service_product_service_integration_id

-- DROP INDEX IF EXISTS public.idx_rev_service_product_service_integration_id;

CREATE INDEX IF NOT EXISTS idx_rev_service_product_service_integration_id
    ON public.rev_service_product USING btree
    (service_id ASC NULLS LAST, integration_authentication_id ASC NULLS LAST)
    TABLESPACE pg_default;
-- Index: idx_rev_service_product_status

-- DROP INDEX IF EXISTS public.idx_rev_service_product_status;

CREATE INDEX IF NOT EXISTS idx_rev_service_product_status
    ON public.rev_service_product USING btree
    (status COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default;


CREATE INDEX IF NOT EXISTS idx_rev_service_product_status_is_active
    ON public.rev_service_product USING btree
    (status COLLATE pg_catalog."default" ASC NULLS LAST, is_active ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS public.automation_rule
(
    id serial primary key,
    automation_rule_name character varying(255) COLLATE pg_catalog."default",
    service_provider_id integer NOT NULL,
    description character varying(255) COLLATE pg_catalog."default",
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean NOT NULL,
    service_provider_name character varying(250) COLLATE pg_catalog."default"
);
CREATE INDEX IF NOT EXISTS idx_automation_rule_service_provider_id
    ON public.automation_rule USING btree
    (service_provider_id ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS public.app_file
(
    id serial primary key,
    amazon_file_name character varying(50) COLLATE pg_catalog."default",
    file_name character varying(500) COLLATE pg_catalog."default",
    tenant_id integer NOT NULL,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    created_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    is_active boolean NOT NULL
);

CREATE TABLE IF NOT EXISTS public.automation_get_usage_by_rate_plan
(
    id serial primary key,
    carrier_rate_plan_code character varying(50) COLLATE pg_catalog."default",
    tenant_id integer NOT NULL,
    service_provider_id integer NOT NULL,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp with time zone,
    is_active boolean NOT NULL
);

CREATE TABLE IF NOT EXISTS public.automation_rule_action
(
    id serial primary key,
    automation_rule_action_name character varying(255) COLLATE pg_catalog."default",
    automation_rule_action_code character varying(255) COLLATE pg_catalog."default",
    automation_rule_type integer,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean NOT NULL
);

CREATE TABLE IF NOT EXISTS public.automation_rule_condition
(
    id serial primary key,
    automation_rule_condition_name character varying(255) COLLATE pg_catalog."default",
    automation_rule_type integer,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean NOT NULL
);

CREATE TABLE IF NOT EXISTS public.automation_rule_customer_rate_plan
(
    id serial primary key,
    automation_rule_id integer NOT NULL,
    customer_rate_plan_id integer NOT NULL,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean NOT NULL,
    CONSTRAINT fk_automation_rule FOREIGN KEY (automation_rule_id)
        REFERENCES public.automation_rule (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_customer_rate_plan FOREIGN KEY (customer_rate_plan_id)
        REFERENCES public.customerrateplan (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE IF NOT EXISTS public.automation_rule_customer_rate_plan_to_process
(
    id serial primary key,
    automation_rule_customer_rate_plan_id integer NOT NULL,
    group_number integer NOT NULL,
    created_by character varying(50) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.automation_rule_followup_effective_date_type
(
    id serial primary key,
    name character varying(50) COLLATE pg_catalog."default",
    description character varying(255) COLLATE pg_catalog."default",
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean NOT NULL
);

CREATE TABLE IF NOT EXISTS public.automation_rule_followup
(
    id serial primary key,
    description character varying(100) COLLATE pg_catalog."default",
    rule_followup_effective_date_type_id integer NOT NULL,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean NOT NULL,
    CONSTRAINT fk_rule_followup_effective_date_type FOREIGN KEY (rule_followup_effective_date_type_id)
        REFERENCES public.automation_rule_followup_effective_date_type (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE IF NOT EXISTS public.automation_rule_detail
(
    id serial primary key,
    automation_rule_id integer NOT NULL,
    rule_condition_id integer NOT NULL,
    rule_condition_value character varying(255) COLLATE pg_catalog."default",
    rule_action_id integer NOT NULL,
    rule_action_value character varying(255) COLLATE pg_catalog."default",
    condition_step integer,
    rule_followup_id integer,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean NOT NULL,
    CONSTRAINT fk_automation_rule FOREIGN KEY (automation_rule_id)
        REFERENCES public.automation_rule (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_automation_rule_action FOREIGN KEY (rule_action_id)
        REFERENCES public.automation_rule_action (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_automation_rule_condition FOREIGN KEY (rule_condition_id)
        REFERENCES public.automation_rule_condition (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_automation_rule_followup FOREIGN KEY (rule_followup_id)
        REFERENCES public.automation_rule_followup (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE IF NOT EXISTS public.automation_rule_followup_detail
(
    id serial primary key,
    rule_followup_id integer NOT NULL,
    rule_action_id integer NOT NULL,
    rule_action_value character varying(255) COLLATE pg_catalog."default",
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean NOT NULL,
    CONSTRAINT fk_rule_action FOREIGN KEY (rule_action_id)
        REFERENCES public.automation_rule_action (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_rule_followup FOREIGN KEY (rule_followup_id)
        REFERENCES public.automation_rule_followup (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE IF NOT EXISTS public.automation_rule_log
(
    id serial primary key ,
    automation_rule_id integer NOT NULL,
    rule_condition_id integer NOT NULL,
    rule_action_id integer,
    status character varying(50) ,
    request_body text ,
    device_updated text ,
    response_body text ,
    description text ,
    file_name character varying(100) ,
    follow_up_action_id integer,
    created_by character varying(100),
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) ,
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) ,
    deleted_date timestamp without time zone,
    is_active boolean NOT NULL,
    rule_detail_id integer,
    rule_follow_up_detail_id integer,
    instance_id character varying(50)
);


CREATE TABLE IF NOT EXISTS public.automation_rule_type
(
    id serial primary key ,
    type_name character varying(50) ,
    created_by character varying(100) ,
    created_date timestamp with time zone NOT NULL,
    modified_by character varying(100) ,
    modified_date timestamp with time zone,
    deleted_by character varying(100) ,
    deleted_date timestamp with time zone,
    is_active boolean NOT NULL
);




CREATE TABLE IF NOT EXISTS public.device_status_uploaded_file_detail
(
    id serial primary key ,
    uploaded_file_id integer NOT NULL,
    iccid character varying(50) ,
    status character varying(50) ,
    upload_status character varying(50),
    processed_date timestamp without time zone,
    processed_by character varying(50) ,
    created_by character varying(100) ,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) ,
    modified_date timestamp without time zone,
    deleted_by character varying(100) ,
    deleted_date timestamp without time zone,
    is_active boolean NOT NULL,
    device_status_id integer,
    mdn_zip_code character varying(50) ,
    rate_plan_code character varying(50) ,
    additional_details_json text ,
    CONSTRAINT fk_device_status_uploaded_file_detail_device_status FOREIGN KEY (device_status_id)
        REFERENCES public.device_status (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);


CREATE INDEX idx_device_status_uploaded_file_detail_status_iccid_created_dat
ON public.device_status_uploaded_file_detail (status ASC, iccid ASC, created_date ASC)
INCLUDE (additional_details_json);

CREATE TABLE IF NOT EXISTS public.e_bonding_device
(
    id serial primary key ,
    service_provider_id integer NOT NULL,
    foundation_account_number character varying(30) ,
    billing_account_number character varying(30) ,
    billing_account_name character varying(100) ,
    subscriber_number character varying(20) ,
    subscriber_activated_date date NOT NULL,
    subscriber_status character varying(20) ,
    subscriber_status_effective_date date NOT NULL,
    subscriber_full_name character varying(50) ,
    device_make character varying(50) ,
    device_model character varying(50) ,
    imei character varying(30) ,
    contract_start_date date,
    contract_end_date date,
    device_effective_date date NOT NULL,
    service_type character varying(10)  NOT NULL,
    iccid character varying(50)  NOT NULL,
    rate_plan_name character varying(200) ,
    user_defined_label1 character varying(50),
    user_defined_label2 character varying(50) ,
    user_defined_label3 character varying(50) ,
    user_defined_label4 character varying(50) ,
    os_version character varying(50) ,
    imeisv character varying(50) ,
    technology_type character varying(50) ,
    created_by character varying(100)  NOT NULL,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) ,
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) ,
    deleted_date timestamp without time zone,
    is_active boolean NOT NULL,
    device_sku character varying(50) ,
    rate_plan_friendly_name character varying(200),
    rate_plan_effective_date date,
    rate_plan_sku character varying(50) ,
    billing_address character varying(150) ,
    billing_city character varying(100) ,
    billing_state character varying(50) ,
    billing_zip_code character varying(25) ,
    ctd_data_usage bigint,
    old_ctd_data_usage bigint,
    plan_limit_mb numeric(25,4),
    data_group_id character varying(50) ,
    pool_id character varying(50) ,
    sms_count integer,
    old_sms_count integer,
    minutes_used integer,
    old_minutes_used integer,
    usage_record_count integer NOT NULL,
    last_usage_date timestamp without time zone,
    bill_year integer,
    bill_month integer,
    usage_aggregate_id integer,
    bill_cycle_end_date timestamp without time zone,
    rate_plan_short_name character varying(50)
);


CREATE INDEX idx_e_bonding_device_subscriber_number
ON e_bonding_device (subscriber_number ASC);



CREATE TABLE IF NOT EXISTS public.e_bonding_device_sync_audit
(
    id serial primary key ,
    last_sync_date timestamp without time zone NOT NULL,
    active_count integer,
    suspend_count integer,
    created_by character varying(100) ,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) ,
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) ,
    deleted_date timestamp without time zone,
    is_active boolean NOT NULL,
    bill_year integer,
    bill_month integer,
    service_provider_id integer
);


CREATE TABLE IF NOT EXISTS public.jasper_device_sync_audit
(
    id serial primary key,
    last_sync_date timestamp without time zone NOT NULL,
    activated_count integer,
    activation_ready_count integer,
    deactivated_count integer,
    inventory_count integer,
    retired_count integer,
    test_ready_count integer,
    created_by character varying(100) ,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) ,
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) ,
    deleted_date timestamp without time zone,
    is_active boolean NOT NULL,
    bill_year integer,
    bill_month integer,
    service_provider_id integer
);


CREATE TABLE IF NOT EXISTS public.telegence_device
(
    id serial primary key ,
    service_provider_id integer NOT NULL,
    foundation_account_number character varying(50) ,
    billing_account_number character varying(50) ,
    subscriber_number character varying(50) ,
    subscriber_activated_date timestamp without time zone,
    subscriber_number_status character varying(10) ,
    refresh_timestamp timestamp without time zone,
    single_user_code character varying(50) ,
    single_user_code_description character varying(100) ,
    service_zip_code character varying(50),
    next_bill_cycle_date timestamp without time zone,
    iccid character varying(50) ,
    imei character varying(50) ,
    bill_year integer,
    bill_month integer,
    device_status_id integer,
    old_device_status_id integer,
    rate_plan_name character varying(50) ,
    ctd_data_usage bigint,
    old_ctd_data_usage bigint,
    plan_limit_mb numeric(25,4),
    data_group_id character varying(50) ,
    pool_id character varying(50) ,
    created_by character varying(100) ,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) ,
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    last_activated_date timestamp without time zone,
    deleted_by character varying(100) ,
    deleted_date timestamp without time zone,
    is_active boolean NOT NULL,
    account_number character varying(50) ,
    last_usage_date timestamp without time zone,
    usage_record_count integer NOT NULL,
    device_make character varying(50) ,
    device_model character varying(50),
    contract_status character varying(50) ,
    ban_status character varying(50) ,
    sms_count integer,
    old_sms_count integer,
    minutes_used integer,
    old_minutes_used integer,
    imei_type_id integer,
    usage_aggregate_id integer,
    contact_name character varying(150) ,
    billing_period_id integer,
    device_technology_type character varying(50) ,
    ip_address character varying(50)  ,
    CONSTRAINT telegence_device_usage_aggregate_id_fkey FOREIGN KEY (usage_aggregate_id)
        REFERENCES public.mobility_device_usage_aggregate (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);


CREATE INDEX IF NOT EXISTS idx_telegence_device_bill_year_bill_month
    ON public.telegence_device (bill_year ASC, bill_month ASC)
    INCLUDE (next_bill_cycle_date, service_provider_id);

CREATE INDEX IF NOT EXISTS idx_telegence_device_billing_period_id
    ON public.telegence_device (billing_period_id ASC);

CREATE INDEX IF NOT EXISTS idx_telegence_device_pool_id_single_user_code
    ON public.telegence_device (pool_id ASC, single_user_code ASC)
    INCLUDE (billing_account_number);

CREATE INDEX IF NOT EXISTS idx_telegence_device_service_provider_id_bill_year_bill_month
    ON public.telegence_device (service_provider_id ASC, bill_year ASC, bill_month ASC)
    INCLUDE (next_bill_cycle_date);

CREATE INDEX IF NOT EXISTS idx_telegence_device_subscriber_number
    ON public.telegence_device (subscriber_number ASC);



CREATE TABLE IF NOT EXISTS public.telegence_device_mobility_feature
(
    id serial primary key ,
    telegence_device_id integer NOT NULL,
    mobility_feature_id integer NOT NULL,
    created_by character varying(100) ,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) ,
    modified_date timestamp without time zone,
    last_activated_date timestamp without time zone,
    deleted_by character varying(100) ,
    deleted_date timestamp without time zone,
    is_active boolean NOT NULL,
    CONSTRAINT telegence_device_mobility_feature_mobility_feature_id_fkey FOREIGN KEY (mobility_feature_id)
        REFERENCES public.mobility_feature (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT telegence_device_mobility_feature_telegence_device_id_fkey FOREIGN KEY (telegence_device_id)
        REFERENCES public.telegence_device (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE INDEX IF NOT EXISTS idx_telegence_device_mobility_feature
    ON public.telegence_device_mobility_feature USING btree
    (telegence_device_id ASC NULLS LAST, is_active ASC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_telegence_device_mobility_feature_telegence_device_id
    ON public.telegence_device_mobility_feature (telegence_device_id ASC);

CREATE TABLE IF NOT EXISTS public.telegence_device_sync_audit
(
    id integer ,
    last_sync_date timestamp without time zone NOT NULL,
    active_count integer,
    suspend_count integer,
    created_by character varying(100) ,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) ,
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) ,
    deleted_date timestamp without time zone,
    is_active boolean NOT NULL,
    bill_year integer,
    bill_month integer,
    service_provider_id integer,
    CONSTRAINT telegence_device_sync_audit_pkey PRIMARY KEY (id)
);


CREATE TABLE IF NOT EXISTS public.thing_space_device_sync_audit
(
    id serial primary key ,
    last_sync_date timestamp without time zone NOT NULL,
    pre_active_count integer,
    active_count integer,
    deactive_count integer,
    suspend_count integer,
    pending_resume_count integer,
    pending_mdn_change_count integer,
    pending_prl_update_count integer,
    pending_preactive_count integer,
    pending_activation_count integer,
    pending_deactivation_count integer,
    pending_suspend_count integer,
    pending_service_plan_change_count integer,
    pending_esn_meid_change_count integer,
    pending_account_update_count integer,
    created_by character varying(100) ,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) ,
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) ,
    deleted_date timestamp without time zone,
    is_active boolean NOT NULL,
    bill_year integer,
    bill_month integer,
    service_provider_id integer
);

CREATE TABLE IF NOT EXISTS public.customer_billing_period
(
    id serial primary key,
    bill_year integer NOT NULL,
    bill_month integer NOT NULL,
    created_by character varying(100) ,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) ,
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) ,
    deleted_date timestamp without time zone,
    is_active boolean NOT NULL
);


CREATE TABLE IF NOT EXISTS public.imei_type
(
    id serial primary key,
    name character varying(50) COLLATE pg_catalog."default",
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean NOT NULL
);

CREATE TABLE IF NOT EXISTS public.integration_connection
(
    id serial primary key,
    integration_id integer NOT NULL,
    production_url character varying(250)  NOT NULL,
    sandbox_url character varying(250) ,
    header_content text ,
    http_version_id integer NOT NULL,
    client_id text ,
    client_secret text ,
    created_by character varying(100) ,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) ,
    deleted_date timestamp without time zone,
    modified_by character varying(100) ,
    modified_date timestamp without time zone,
    is_active boolean NOT NULL,
    auth_test_url text ,
    CONSTRAINT integration_connection_integration_id_fkey FOREIGN KEY (integration_id)
        REFERENCES public.integration (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);


CREATE TABLE IF NOT EXISTS public.lnp_device_change
(
    id bigserial primary key,
    bulk_change_id bigint NOT NULL,
    phone_number character varying(50) COLLATE pg_catalog."default" NOT NULL,
    change_request text COLLATE pg_catalog."default",
    bandwidth_telephone_number_id integer,
    is_processed boolean NOT NULL,
    has_errors boolean NOT NULL,
    status character varying(50) COLLATE pg_catalog."default" NOT NULL,
    status_details text COLLATE pg_catalog."default",
    processed_date timestamp without time zone,
    processed_by character varying(50) COLLATE pg_catalog."default",
    created_by character varying(100) COLLATE pg_catalog."default" NOT NULL,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean NOT NULL,
    device_change_request_id bigint
);
CREATE INDEX IF NOT EXISTS idx_lnp_device_change_is_processed_bulkchangeid
    ON public.lnp_device_change (is_processed ASC NULLS LAST, bulk_change_id ASC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_lnp_device_change_phone_number_status
    ON public.lnp_device_change (phone_number, status);
CREATE INDEX IF NOT EXISTS idx_lnp_device_change_bulkchangeid_processed
    ON public.lnp_device_change (bulk_change_id ASC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_lnp_device_change_change_request_device_change_request_id_i
    ON public.lnp_device_change (device_change_request_id, id);
CREATE INDEX IF NOT EXISTS idx_lnp_device_change_is_active
    ON public.lnp_device_change (is_active ASC NULLS LAST);

CREATE TABLE IF NOT EXISTS public.device_status_uploaded_file
(
    id serial primary key,
    service_provider_id integer NOT NULL,
    file_name character varying(255)  NOT NULL,
    status character varying(50)  NOT NULL,
    description character varying(50) ,
    processed_date timestamp without time zone,
    processed_by character varying(50) ,
    created_by character varying(100)  NOT NULL,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) ,
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) ,
    deleted_date timestamp without time zone,
    is_active boolean NOT NULL DEFAULT true,
    retired_count integer NOT NULL,
    card_count integer NOT NULL,
    processed_count integer NOT NULL,
    error_count integer NOT NULL,
    rev_customer_id character varying(50) ,
    tenant_id integer,
    app_file_id integer,
    customer_id integer
);
CREATE TABLE IF NOT EXISTS public.smi_change_type_integration
(
    id serial primary key,
    change_request_type_id integer,
    change_request_type character varying(100),
    integration_id integer
);
CREATE TABLE IF NOT EXISTS public.qualification
(
    id bigserial primary key,
    qualification_id character varying(50) COLLATE pg_catalog."default",
    is_qualified boolean NOT NULL,
    customer_id character varying(50) COLLATE pg_catalog."default",
    service_line_id character varying(50) COLLATE pg_catalog."default",
    address_line character varying(100) COLLATE pg_catalog."default",
    city character varying(50) COLLATE pg_catalog."default",
    state character varying(50) COLLATE pg_catalog."default",
    country character varying(50) COLLATE pg_catalog."default",
    postal_code character varying(50) COLLATE pg_catalog."default",
    device_bulk_change_id bigint,
    tenant_id integer NOT NULL,
    status character varying(50) COLLATE pg_catalog."default",
    created_by character varying(100) COLLATE pg_catalog."default" NOT NULL,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp with time zone,
    is_active boolean NOT NULL
);

CREATE TABLE IF NOT EXISTS public.qualification_address (
    id BIGSERIAL PRIMARY KEY,
    qualification_id BIGINT NOT NULL,
    qualification_token VARCHAR(100),
    is_qualified BOOLEAN NOT NULL DEFAULT FALSE,
    customer_id VARCHAR(100),
    street_number VARCHAR(100),
    street_name VARCHAR(100),
    city VARCHAR(100),
    state VARCHAR(100),
    zip_code VARCHAR(100),
    country VARCHAR(100),
    created_by VARCHAR(100) NOT NULL,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_by VARCHAR(100),
    modified_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    deleted_by VARCHAR(100),
    deleted_date TIMESTAMPTZ,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    is_activated_by_service BOOLEAN NOT NULL DEFAULT FALSE,
    status VARCHAR(100)
);

CREATE TABLE IF NOT EXISTS public.qualification_log
(
    id bigserial primary key,
    qualification_id bigint NOT NULL,
    qualification_address_id bigint,
    level character varying(50) COLLATE pg_catalog."default" NOT NULL,
    message text COLLATE pg_catalog."default" NOT NULL,
    request text COLLATE pg_catalog."default" NOT NULL,
    response text COLLATE pg_catalog."default" NOT NULL,
    created_by character varying(100) COLLATE pg_catalog."default" NOT NULL,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.e_bonding_device_mobility_feature
(
    id serial primary key,
    e_bonding_device_id integer NOT NULL,
    mobility_feature_id integer NOT NULL,
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    last_activated_date timestamp without time zone,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean NOT NULL,
    CONSTRAINT fk_ebondingdevice FOREIGN KEY (e_bonding_device_id)
        REFERENCES public.e_bonding_device (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_mobility_feature FOREIGN KEY (mobility_feature_id)
        REFERENCES public.mobility_feature (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);
CREATE INDEX IF NOT EXISTS idx_ebonding_device_mobility_feature_ebonding_device_id
    ON public.e_bonding_device_mobility_feature USING btree
    (e_bonding_device_id ASC NULLS LAST)
    TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS public.awsdms_ddl_audit
(
    c_key bigserial primary key,
    c_time timestamp without time zone,
    c_user character varying(64) COLLATE pg_catalog."default",
    c_txn character varying(16) COLLATE pg_catalog."default",
    c_tag character varying(24) COLLATE pg_catalog."default",
    c_oid integer,
    c_name character varying(64) COLLATE pg_catalog."default",
    c_schema character varying(64) COLLATE pg_catalog."default",
    c_ddlqry text COLLATE pg_catalog."default"
);

CREATE TABLE IF NOT EXISTS public.device_change_request
(
    id bigserial primary key,
    bulk_change_id bigint NOT NULL,
    change_request text COLLATE pg_catalog."default",
    created_by character varying(100) COLLATE pg_catalog."default" NOT NULL,
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp with time zone,
    is_active boolean NOT NULL
);

CREATE TABLE IF NOT EXISTS public.smi_communication_plan_carrier_rate_plan
(
    id serial primary key,
    communication_plan_id integer,
    communication_plan_name character varying COLLATE pg_catalog."default",
    rate_plan_id integer,
    carrier_rate_plan_name character varying COLLATE pg_catalog."default",
    created_by character varying(100) COLLATE pg_catalog."default",
    created_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    modified_by character varying(100) COLLATE pg_catalog."default",
    modified_date timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    deleted_by character varying(100) COLLATE pg_catalog."default",
    deleted_date timestamp without time zone,
    is_active boolean default true,
    rate_plan_code character varying(256) COLLATE pg_catalog."default",
    CONSTRAINT fk_communication_plan FOREIGN KEY (communication_plan_id)
        REFERENCES public.sim_management_communication_plan (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_rate_plan FOREIGN KEY (rate_plan_id)
        REFERENCES public.carrier_rate_plan (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE IF NOT EXISTS public.inventory_actions_urls (
    id SERIAL PRIMARY KEY,
    service_provider_id VARCHAR,
    action VARCHAR,
    main_url VARCHAR,
    session_url VARCHAR,
    token_url VARCHAR,
    queue_url VARCHAR
);

CREATE TABLE IF NOT EXISTS public.mobility_configuration_change_queue (
    id SERIAL PRIMARY KEY,
    device_id INTEGER,
    mobility_device_id INTEGER,
    service_provider_id INTEGER NOT NULL,
    mobility_configuration_change_details TEXT NOT NULL,
    is_processed BOOLEAN NOT NULL,
    created_by VARCHAR(100) NOT NULL,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_by VARCHAR(100),
    modified_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_activated_date TIMESTAMP,
    deleted_by VARCHAR(100),
    deleted_date TIMESTAMP,
    is_active BOOLEAN NOT NULL,
    tenant_id INTEGER,
    CONSTRAINT fk_mobility_device FOREIGN KEY (mobility_device_id)
        REFERENCES public.mobility_device (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION,
    CONSTRAINT fk_service_provider FOREIGN KEY (service_provider_id)
        REFERENCES public.serviceprovider (id)
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE IF NOT EXISTS public.open_search_index (
    id SERIAL PRIMARY KEY,
    search_module VARCHAR(100),
    search_tables VARCHAR(100),
    search_cols JSONB DEFAULT '[]',
    relations JSONB DEFAULT '{}',
    index_search_type VARCHAR(50),
    db_name VARCHAR,
    module_name VARCHAR
);

CREATE TABLE IF NOT EXISTS public.optimization_details (
    id SERIAL PRIMARY KEY,
    optimization_session_id VARCHAR,
    progress VARCHAR,
    error_message VARCHAR,
    customer_id VARCHAR,
    session_id uuid
);

CREATE TABLE IF NOT EXISTS public.optimization_setting (
    id SERIAL PRIMARY KEY,
    auto_update_rateplans BOOLEAN,
    carrier_optimization_email_subject TEXT,
    carrier_optimization_from_email_address TEXT,
    carrier_optimization_ou TEXT,
    carrier_optimization_to_email_address TEXT,
    customer_optimization_email_subject TEXT,
    customer_optimization_from_email_address TEXT,
    customer_optimization_to_email_address TEXT,
    go_for_rateplan_update_email_subject TEXT,
    linux_timezone TEXT,
    no_go_for_rate_plan_update_email_subject TEXT,
    optimization_bcc_email_address TEXT,
    optimization_sync_device_error_email_subject TEXT,
    optino_continous_last_day_optimization BOOLEAN,
    optino_cross_providercustomer_optimization BOOLEAN,
    using_new_process_in_customer_charge BOOLEAN,
    windows_timezone TEXT,
    tenant_id INTEGER,
    modified_by VARCHAR(100),
    created_by VARCHAR(100),
    created_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    modified_date TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS public.rule_rule_definition (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    object_type_id UUID NOT NULL,
    rule_id VARCHAR(100),
    name VARCHAR(100) NOT NULL,
    priority INT NOT NULL DEFAULT 1,
    subject_line VARCHAR(250),
    display_message VARCHAR(250) NOT NULL,
    notes VARCHAR(500),
    order_of_execution INT NOT NULL DEFAULT 1,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100) NOT NULL,
    modified_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    modified_by VARCHAR(100),
    deleted_date TIMESTAMP,
    deleted_by VARCHAR(100),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    send_to_carrier BOOLEAN NOT NULL DEFAULT TRUE,
    send_to_customer BOOLEAN NOT NULL DEFAULT FALSE,
    should_show_projected_usage_cost BOOLEAN NOT NULL DEFAULT TRUE,
    version_no VARCHAR,
    version_update_purpose VARCHAR,
    rule_id_1_0 UUID,
    rule_def_id VARCHAR,
    service_provider VARCHAR,
    customers_list TEXT,
    expression_ids TEXT,
    expression_names TEXT
);

CREATE TABLE IF NOT EXISTS public.thing_space_device (
    id SERIAL PRIMARY KEY,
    iccid VARCHAR(50),
    imei VARCHAR(50) ,
    status VARCHAR(50) ,
    rate_plan VARCHAR(50) ,
    communication_plan VARCHAR(50) ,
    last_usage_date TIMESTAMP WITHOUT TIME ZONE,
    primary_place_of_use_first_name VARCHAR(50) ,
    primary_place_of_use_middle_name VARCHAR(50) ,
    primary_place_of_use_last_name VARCHAR(50),
    thing_space_ppu TEXT,
    created_by VARCHAR(100),
    created_date TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    modified_by VARCHAR(100) ,
    modified_date TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_activated_date TIMESTAMP WITHOUT TIME ZONE,
    deleted_by VARCHAR(100) ,
    deleted_date TIMESTAMP WITHOUT TIME ZONE,
    is_active BOOLEAN NOT NULL,
    device_status_id INTEGER,
    ip_address VARCHAR(50) ,
    service_provider_id INTEGER NOT NULL,
    CONSTRAINT fk_thingspace_device_service_provider FOREIGN KEY (service_provider_id)
        REFERENCES public.serviceprovider (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE IF NOT EXISTS public.thing_space_device_detail
(
    id SERIAL PRIMARY KEY,
    iccid VARCHAR(50) NOT NULL,
    apn VARCHAR(250),
    "package" VARCHAR(250),
    bill_year INTEGER,
    bill_month INTEGER,
    created_by VARCHAR(100) NOT NULL,
    created_date TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    modified_by VARCHAR(100),
    modified_date TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_activated_date TIMESTAMP WITHOUT TIME ZONE,
    deleted_by VARCHAR(100),
    deleted_date TIMESTAMP WITHOUT TIME ZONE,
    is_active BOOLEAN NOT NULL,
    account_number VARCHAR(50),
    thing_space_date_added TIMESTAMP WITHOUT TIME ZONE,
    thing_space_date_activated TIMESTAMP WITHOUT TIME ZONE,
    billing_cycle_end_date TIMESTAMP WITHOUT TIME ZONE,
    service_provider_id INTEGER,
    CONSTRAINT fk_thingspace_device_detail_service_provider FOREIGN KEY (service_provider_id)
        REFERENCES public.serviceprovider (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE IF NOT EXISTS public.thing_space_device_usage
(
    id SERIAL PRIMARY KEY,
    iccid VARCHAR(150),
    imsi VARCHAR(150),
    msisdn VARCHAR(150),
    imei VARCHAR(150),
    status VARCHAR(150),
    rate_plan VARCHAR(150),
    communication_plan VARCHAR(255),
    ctd_data_usage BIGINT,
    ctd_sms_usage BIGINT,
    ctd_voice_usage BIGINT,
    ctd_session_count INTEGER,
    overage_limit_reached BOOLEAN,
    overage_limit_override VARCHAR(50),
    bill_year INTEGER,
    bill_month INTEGER,
    created_by VARCHAR(100) NOT NULL,
    created_date TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    modified_by VARCHAR(100),
    modified_date TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    deleted_by VARCHAR(100),
    deleted_date TIMESTAMP WITHOUT TIME ZONE,
    is_active BOOLEAN NOT NULL,
    device_status_id INTEGER,
    old_ctd_data_usage BIGINT,
    old_device_status_id INTEGER,
    service_provider_id INTEGER,
    CONSTRAINT fk_thingspace_device_usage_device_status FOREIGN KEY (device_status_id)
        REFERENCES public.device_status (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
);

CREATE TABLE if not exists rev_package_product (
    id SERIAL PRIMARY KEY,
    package_product_id INTEGER NOT NULL,
    product_id INTEGER,
    package_id INTEGER,
    description VARCHAR(1024),
    code1 VARCHAR(1024),
    code2 VARCHAR(1024),
    rate NUMERIC(25, 4),
    cost NUMERIC(25, 4),
    buy_rate NUMERIC(25, 4),
    quantity INTEGER,
    tax_included BOOLEAN,
    group_on_bill BOOLEAN,
    itemized BOOLEAN,
    credit BOOLEAN,
    created_by VARCHAR(100) NOT NULL,
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL,
    modified_by VARCHAR(100),
    modified_date TIMESTAMP WITHOUT TIME ZONE,
    deleted_by VARCHAR(100),
    deleted_date TIMESTAMP WITHOUT TIME ZONE,
    is_active BOOLEAN DEFAULT TRUE NOT NULL,
    integration_authentication_id INTEGER,
    rev_product_id INTEGER,
    rev_package_id INTEGER,
    CONSTRAINT fk_rev_package_product_integration_authentication FOREIGN KEY (integration_authentication_id)
        REFERENCES integration_authentication(id),
    CONSTRAINT fk_rev_package_product_rev_package FOREIGN KEY (rev_package_id)
        REFERENCES rev_package(id),
    CONSTRAINT fk_rev_package_product_rev_product FOREIGN KEY (rev_product_id)
        REFERENCES rev_product(id)
);






