--
-- PostgreSQL database dump
--

\connect - postgres

SET search_path = public, pg_catalog;

--
-- TOC entry 2 (OID 72395)
-- Name: object; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE object (
    odba_id integer,
    content text,
    name text
);


--
-- TOC entry 3 (OID 72395)
-- Name: object; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE object FROM PUBLIC;
GRANT ALL ON TABLE object TO odbauser;


--
-- TOC entry 4 (OID 174743)
-- Name: odba_index; Type: INDEX; Schema: public; Owner: postgres
--

CREATE INDEX odba_index ON object USING btree (odba_id);
alter table object add constraint name_unique  unique(name);


