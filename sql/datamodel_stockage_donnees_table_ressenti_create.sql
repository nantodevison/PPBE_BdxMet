/* =====================================================
 * SCRIPT DE DEFINITION DES DONNEES DE MESURES RESSENTI
 =======================================================*/

CREATE SCHEMA ressenti ;
-- DROP SCHEMA ressenti CASCADE ;

/* ----------
 * DATA TYPES
 ----------- */

CREATE type ressenti.enum_genre AS ENUM ('Masculin', 'Féminin') ;
CREATE type ressenti.enum_vehicule_source AS ENUM ('Véhicules légers (voiture, camionnette, ...)', 
'2 roues motorisées (scooter, motos, ...)',
'Poids Lourds (camion, bus, ...)', 'Autre') ;
CREATE type ressenti.enum_type_bati AS ENUM ('Maison individuelle', 'Habitat collectif') ;
CREATE TYPE ressenti.enum_source_bruit AS ENUM ('Bruit routier', 'Bruit aérien', 'Bruit de voisinage', 'Bruit de chantier', 'Autre');
CREATE TYPE ressenti.enum_qualif_gene AS ENUM ('Permanent', 'Intermittent', 'Fort', 'Faible', 'Net', 'Étouffé', 'Bref', 'Long', 'Variable', 
'Constant', 'Aigu', 'Grave', 'Progressif', 'Soudain', 'Sourd', 'Strident', 'Lointain', 'Proche', 'Autre') ;
CREATE TYPE ressenti.enum_route_source AS ENUM ('Rocade', 'Boulevard de l''entre-deux Mers (Ex RD936)', 'Avenue Hubert Dubedout', 
'Rue Salvador Allende', 'Autre');
CREATE TYPE ressenti.enum_localisation AS ENUM ('Dans mon logement', 'Dans mon jardin / sur ma terrasse / sur mon balcon', 'Dans la rue', 'Autre');
CREATE TYPE ressenti.enum_periode_travail AS ENUM ('Autre', 'journée (entre 8h et 18h environ)', 'matin (entre 6h et 14h environ)', 
'après-midi (entre 6h et 14h environ)', 'nuit (entre 22h et 6h environ)', 'Retraite') ;


/* --------------------
 * TABLE D'ENUMERATION
 ----------------------*/

CREATE TABLE ressenti.enum_emploi (
    code text PRIMARY KEY);
INSERT INTO ressenti.enum_emploi 
VALUES 
('10 - Agriculteurs exploitants'), ('21 - Artisans'), ('22 - Commerçants et assimilés'), ('23 - Chefs d''entreprise de 10 salariés ou plus'), 
('31 - Professions libérales et assimilés'), ('32 - Cadres de la fonction publique, professions intellectuelles et  artistiques'), 
('36 - Cadres d''entreprise'), ('41 - Professions intermédiaires de l''enseignement, de la santé, de la fonction publique et assimilés'), 
('46 - Professions intermédiaires administratives et commerciales des entreprises'), ('47 - Techniciens'), 
('48 - Contremaîtres, agents de maîtrise'), ('51 - Employés de la fonction publique'), ('54 - Employés administratifs d''entreprise'), 
('55 - Employés de commerce'), ('56 - Personnels des services directs aux particuliers'), ('61 - Ouvriers qualifiés'), 
('66 - Ouvriers non qualifiés'), ('69 - Ouvriers agricoles'), ('71 - Anciens agriculteurs exploitants'), 
('72 - Anciens artisans, commerçants, chefs d''entreprise'), ('73 - Anciens cadres et professions intermédiaires'), 
('76 - Anciens employés et ouvriers'), ('81 - Chômeurs n''ayant jamais travaillé'), ('82 - Inactifs divers (autres que retraités)'), ('Retraités') ;


CREATE TABLE ressenti.enum_note_sensib_gene (
    code integer NOT NULL,
    definition text,
    PRIMARY KEY (code)); 
INSERT INTO ressenti.enum_note_sensib_gene(code, definition)
VALUES (0, 'pas du tout sensible'), (1, NULL), (2, NULL), (3, NULL), (4, NULL), (5, NULL), (6, NULL), (7, NULL), (8, NULL), 
(9, NULL), (10, 'extrêmement sensible')  ;


/* -----------------
 * TABLES DE DONNEES 
 ---------------- */

CREATE TABLE ressenti.participant (
    id integer NOT NULL ,
    adresse text NOT NULL,
    genre ressenti.enum_genre,
    age integer,
    emploi text,
    sensibilite_bruit integer,
    periode_travail ressenti.enum_periode_travail,
    periode_travail_autre text,
    sensib_bruit_travail integer,
    gene_long_terme integer,
    gene_long_terme_6_18 integer,
    gene_long_terme_18_22 integer,
    gene_long_terme_22_6 integer,
    bati_type ressenti.enum_type_bati,
    bati_annee integer,
    perturbation TEXT,
    papier boolean NOT NULL,
    PRIMARY KEY (id));
CREATE INDEX ON ressenti.participant
    (sensibilite_bruit);
CREATE INDEX ON ressenti.participant
    (sensib_bruit_travail);
CREATE INDEX ON ressenti.participant
    (gene_long_terme);
CREATE INDEX ON ressenti.participant
    (gene_long_terme_6_18);
CREATE INDEX ON ressenti.participant
    (gene_long_terme_18_22);
CREATE INDEX ON ressenti.participant
    (gene_long_terme_22_6);


CREATE TABLE ressenti.situ_gene (
    id serial,
    id_participant integer NOT NULL,
    debut_gene timestamp without time zone NOT NULL,
    fin_gene timestamp without time zone NOT NULL,
    duree_gene INTERVAL,
    note_gene integer NOT NULL,
    source_bruit TEXT[],
    source_bruit_coment TEXT,
    route_source TEXT[],
    route_source_coment TEXT,
    vehicule_source TEXT[],
    localisation_gene TEXT[] NOT NULL,
    qualif_bruit TEXT[],
    coment text,
    PRIMARY KEY (id));
CREATE INDEX ON ressenti.situ_gene
    (id_participant);
CREATE INDEX ON ressenti.situ_gene
    (note_gene);
-- A faire post import : 
/*ALTER TABLE ressenti.situ_gene ALTER COLUMN route_source TYPE ressenti.enum_route_source[] USING route_source::ressenti.enum_route_source[] ;
ALTER TABLE ressenti.situ_gene ALTER COLUMN source_bruit TYPE ressenti.enum_source_bruit[] USING source_bruit::ressenti.enum_source_bruit[] ;
ALTER TABLE ressenti.situ_gene ALTER COLUMN vehicule_source TYPE ressenti.enum_vehicule_source[] USING vehicule_source::ressenti.enum_vehicule_source[] ;
ALTER TABLE ressenti.situ_gene ALTER COLUMN qualif_bruit TYPE ressenti.enum_qualif_gene[] USING qualif_bruit::ressenti.enum_qualif_gene[] ;
ALTER TABLE ressenti.situ_gene ALTER COLUMN localisation_gene TYPE ressenti.enum_localisation[] USING localisation_gene::ressenti.enum_localisation[] ;*/



CREATE TABLE ressenti.situ_gene_KO (
    id serial,
    id_participant integer NOT NULL,
    debut_gene timestamp without time zone,
    fin_gene timestamp without time zone,
    duree_gene INTERVAL,
    note_gene integer,
    source_bruit TEXT[],
    source_bruit_coment TEXT,
    route_source TEXT[],
    route_source_coment TEXT,
    vehicule_source TEXT[],
    localisation_gene TEXT[],
    qualif_bruit TEXT[],
    coment text,
    PRIMARY KEY (id));
CREATE INDEX ON ressenti.situ_gene_KO
    (id_participant);
CREATE INDEX ON ressenti.situ_gene_KO
    (note_gene);
/*ALTER TABLE ressenti.situ_gene_ko ALTER COLUMN route_source TYPE ressenti.enum_route_source[] USING route_source::ressenti.enum_route_source[] ;
ALTER TABLE ressenti.situ_gene_ko ALTER COLUMN source_bruit TYPE ressenti.enum_source_bruit[] USING source_bruit::ressenti.enum_source_bruit[] ;
ALTER TABLE ressenti.situ_gene_ko ALTER COLUMN vehicule_source TYPE ressenti.enum_vehicule_source[] USING vehicule_source::ressenti.enum_vehicule_source[] ;
ALTER TABLE ressenti.situ_gene_ko ALTER COLUMN qualif_bruit TYPE ressenti.enum_qualif_gene[] USING qualif_bruit::ressenti.enum_qualif_gene[] ;
ALTER TABLE ressenti.situ_gene_ko ALTER COLUMN localisation_gene TYPE ressenti.enum_localisation[] USING localisation_gene::ressenti.enum_localisation[] ;*/

/* ---------------
 * CONTAINTES
 -------------- */

ALTER TABLE ressenti.participant ADD CONSTRAINT FK_participant__sensibilite_bruit FOREIGN KEY (sensibilite_bruit) REFERENCES ressenti.enum_note_sensib_gene(code);
ALTER TABLE ressenti.participant ADD CONSTRAINT FK_participant__sensib_bruit_travail FOREIGN KEY (sensib_bruit_travail) REFERENCES ressenti.enum_note_sensib_gene(code);
ALTER TABLE ressenti.participant ADD CONSTRAINT FK_participant__gene_long_terme FOREIGN KEY (gene_long_terme) REFERENCES ressenti.enum_note_sensib_gene(code);
ALTER TABLE ressenti.participant ADD CONSTRAINT FK_participant__gene_long_terme_6_18 FOREIGN KEY (gene_long_terme_6_18) REFERENCES ressenti.enum_note_sensib_gene(code);
ALTER TABLE ressenti.participant ADD CONSTRAINT FK_participant__gene_long_terme_18_22 FOREIGN KEY (gene_long_terme_18_22) REFERENCES ressenti.enum_note_sensib_gene(code);
ALTER TABLE ressenti.participant ADD CONSTRAINT FK_participant__gene_long_terme_22_6 FOREIGN KEY (gene_long_terme_22_6) REFERENCES ressenti.enum_note_sensib_gene(code);
ALTER TABLE ressenti.situ_gene ADD CONSTRAINT FK_situ_gene__id_participant FOREIGN KEY (id_participant) REFERENCES ressenti.participant(id);
ALTER TABLE ressenti.situ_gene ADD CONSTRAINT FK_situ_gene__note_gene FOREIGN KEY (note_gene) REFERENCES ressenti.enum_note_sensib_gene(code);
ALTER TABLE ressenti.situ_gene_KO ADD CONSTRAINT FK_situ_gene_KO__id_participant FOREIGN KEY (id_participant) REFERENCES ressenti.participant(id);
ALTER TABLE ressenti.situ_gene_KO ADD CONSTRAINT FK_situ_gene_KO__note_gene FOREIGN KEY (note_gene) REFERENCES ressenti.enum_note_sensib_gene(code);