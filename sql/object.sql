--
-- PostgreSQL database dump
--

\connect - postgres

SET search_path = public, pg_catalog;

-- TOC entry 4 (OID 1608529)
-- Name: object; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE object (
    odba_id integer NOT NULL,
    content text,
    name text,
    prefetchable boolean
);


--
-- TOC entry 5 (OID 1608529)
-- Name: object; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE object FROM PUBLIC;
GRANT ALL ON TABLE object TO odbauser;


--
-- TOC entry 6 (OID 1639333)
-- Name: object_connection; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE object_connection (
    origin_id integer,
    target_id integer
);


--
-- TOC entry 7 (OID 1639333)
-- Name: object_connection; Type: ACL; Schema: public; Owner: postgres
--

REVOKE ALL ON TABLE object_connection FROM PUBLIC;
GRANT ALL ON TABLE object_connection TO odbauser;

ALTER TABLE ONLY object
    ADD CONSTRAINT object_pkey PRIMARY KEY (odba_id);

--
-- TOC entry 11 (OID 1643198)
-- Name: origin_id; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY object_connection
    ADD CONSTRAINT origin_id FOREIGN KEY (origin_id) REFERENCES object(odba_id) ON UPDATE NO ACTION ON DELETE CASCADE;


--
-- TOC entry 12 (OID 1643202)
-- Name: target_id; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY object_connection
    ADD CONSTRAINT target_id FOREIGN KEY (target_id) REFERENCES object(odba_id) ON UPDATE NO ACTION ON DELETE CASCADE;


