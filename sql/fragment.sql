DROP SCHEMA IF EXISTS lab2 CASCADE;
CREATE SCHEMA lab2;

CREATE TABLE lab2.currency (
    cur_uid  smallint PRIMARY KEY,
    iso_code char(3) NOT NULL
);

CREATE TABLE lab2.category (
    cat_uid    integer PRIMARY KEY,
    parent_uid integer REFERENCES lab2.category(cat_uid),
    slug       text NOT NULL,
    name       text NOT NULL
);

CREATE TABLE lab2.client (
    cp_uid       bigint PRIMARY KEY,
    email        text NOT NULL,
    company_name text,
    country      char(2),
    rating_avg   numeric(3,2)
);

CREATE TABLE lab2.project (
    p_uid        bigint PRIMARY KEY,
    cp_uid       bigint NOT NULL REFERENCES lab2.client(cp_uid),
    cat_uid      integer REFERENCES lab2.category(cat_uid),
    cur_uid      smallint NOT NULL REFERENCES lab2.currency(cur_uid),
    title        text NOT NULL,
    description  text NOT NULL,
    pricing_type text NOT NULL CHECK (pricing_type IN ('FIXED','HOURLY')),
    budget_min   numeric(14,2),
    budget_max   numeric(14,2),
    status       text NOT NULL CHECK (status IN ('DRAFT','OPEN','IN_PROGRESS','CLOSED','CANCELLED')),
    created_at   timestamptz NOT NULL,
    deadline     date,
    is_deleted   boolean NOT NULL DEFAULT false,
    skills       text[] NOT NULL DEFAULT '{}',
    attrs        jsonb  NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE lab2.proposal (
    pr_uid     bigint PRIMARY KEY,
    p_uid      bigint NOT NULL REFERENCES lab2.project(p_uid),
    bid_amount numeric(14,2) NOT NULL,
    created_at timestamptz NOT NULL
);

CREATE INDEX ix_project_feed   ON lab2.project (status, cat_uid, created_at DESC) WHERE is_deleted = false;
CREATE INDEX ix_project_budget ON lab2.project (budget_max) WHERE status = 'OPEN';
CREATE INDEX ix_project_skills ON lab2.project USING gin (skills) WHERE status = 'OPEN';
CREATE INDEX ix_proposal_p_uid ON lab2.proposal (p_uid);

CREATE VIEW lab2.v_project_card AS
SELECT p.p_uid, p.title, p.created_at, p.deadline, p.status,
       p.budget_min, p.budget_max, p.skills, p.attrs, p.cat_uid,
       cat.slug        AS category_slug,
       cl.company_name AS company_name,
       cur.iso_code    AS currency,
       (SELECT count(*) FROM lab2.proposal pr WHERE pr.p_uid = p.p_uid) AS proposals_count,
       jsonb_build_object(
           '_id', p.p_uid,
           'schemaVersion', 2,
           'title', p.title,
           'description', p.description,
           'pricing', jsonb_build_object(
               'type', p.pricing_type, 'min', p.budget_min,
               'max', p.budget_max, 'currency', cur.iso_code),
           'status', p.status,
           'createdAt', p.created_at,
           'deadline', p.deadline,
           'category', jsonb_build_object(
               'id', p.cat_uid, 'slug', cat.slug, 'name', cat.name,
               'path', CASE WHEN par.slug IS NULL
                            THEN jsonb_build_array(cat.slug)
                            ELSE jsonb_build_array(par.slug, cat.slug) END),
           'client', jsonb_build_object(
               'id', p.cp_uid, 'company', cl.company_name,
               'country', cl.country, 'email', cl.email, 'rating', cl.rating_avg),
           'skills', to_jsonb(p.skills),
           'attrs', p.attrs,
           'stats', jsonb_build_object('proposals',
               (SELECT count(*) FROM lab2.proposal pr WHERE pr.p_uid = p.p_uid))
       ) AS card
  FROM lab2.project p
  JOIN lab2.client   cl  ON cl.cp_uid   = p.cp_uid
  JOIN lab2.currency cur ON cur.cur_uid = p.cur_uid
  LEFT JOIN lab2.category cat ON cat.cat_uid = p.cat_uid
  LEFT JOIN lab2.category par ON par.cat_uid = cat.parent_uid
 WHERE p.is_deleted = false;

CREATE MATERIALIZED VIEW lab2.mv_project_card AS
SELECT * FROM lab2.v_project_card WITH NO DATA;
CREATE UNIQUE INDEX ix_mv_project_card_uid ON lab2.mv_project_card (p_uid);
CREATE INDEX ix_mv_feed   ON lab2.mv_project_card (status, category_slug, created_at DESC);
CREATE INDEX ix_mv_skills ON lab2.mv_project_card USING gin (skills);
