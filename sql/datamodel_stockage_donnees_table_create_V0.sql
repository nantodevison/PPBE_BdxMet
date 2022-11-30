/* =================================
 * SCRIPT DE DEFINITION DES DONNEES
 ==================================*/

CREATE SCHEMA mesures_physiques ;

CREATE TABLE mesures_physiques.enum_nature_materiel (
    nature varchar NOT NULL,
    PRIMARY KEY (nature)
);
INSERT INTO mesures_physiques.enum_nature_materiel (nature) VALUES
    ('sonometre'),('station_meteo'),('compteur_trafic') ;


CREATE TABLE mesures_physiques.enum_classe_materiel (
    classe varchar(100) NOT NULL,
    PRIMARY KEY (classe)
);
INSERT INTO mesures_physiques.enum_classe_materiel (classe) VALUES
    ('integrateur classe 1'),('boucle SIREDO'),('radar'), ('temperature et vent sur 2 hauteur') ;

CREATE TABLE mesures_physiques.enum_marque (
    marque varchar(50) NOT NULL,
    PRIMARY KEY (marque)
);
INSERT INTO mesures_physiques.enum_marque (marque) VALUES
    ('01dB'),('Hobo'),('TagMaster') ;

CREATE TABLE mesures_physiques.enum_modele (
    modele varchar(50) NOT NULL,
    PRIMARY KEY (modele) 
);
INSERT INTO mesures_physiques.enum_modele (modele) VALUES
    ('fusion'),('onset'),('blackcat') ;

CREATE TABLE mesures_physiques.materiel_metrologique (
    id serial,
    marque varchar(254) NOT NULL,
    modele varchar(254) NOT NULL,
    nature varchar(100) NOT NULL,
    classe varchar(100) NOT NULL,
    obs TEXT,
    PRIMARY KEY (id)
);
INSERT INTO mesures_physiques.materiel_metrologique (marque, modele, nature, classe, obs) VALUES
('01dB', 'fusion', 'sonometre', 'integrateur classe 1', 'num de serie : 11255'),
('01dB', 'fusion', 'sonometre', 'integrateur classe 1', 'num de serie : 11356'),
('01dB', 'fusion', 'sonometre', 'integrateur classe 1', 'num de serie : 12527'),
('Hobo', 'onset', 'station_meteo', 'temperature et vent sur 2 hauteur', 'hauteur 1m : temp + hygro + anemo + direction vent ; hauteur 3m : temp + hygro + anemo + direction vent ; autre : rayonnement + pluvio'),
('TagMaster', 'blackcat', 'compteur_trafic', 'radar', 'posé par société CPEV') ;

CREATE INDEX ON mesures_physiques.materiel_metrologique
    (nature);
CREATE INDEX ON mesures_physiques.materiel_metrologique
    (classe);


CREATE TABLE mesures_physiques.site_mesure (
    id serial,
    nom text NOT NULL,
    x_wgs84 numeric NOT NULL,
    y_wgs84 numeric NOT NULL,
    PRIMARY KEY (id)
);
INSERT INTO mesures_physiques.site_mesure (nom, x_wgs84, y_wgs84) VALUES
    ('10 rue Pierre Ronsard, Floirac', 44.84397481710526, -0.506162593871255),
    ('8 rue des Noyers , Floirac', 44.846011455631015, -0.5034000443993129),
    ('22 rue Jules Ladoumegue, Floirac', 44.83939758942005, -0.5027128569781751),
    ('26 rue Francois Villon, Floirac', 44.84379570806992, -0.5045866417334064),
    ('Jardiland, Artigues-Pres-Bordeaux', 44.843395902840236, -0.4990775992204415),
    ('ex D936', 44.84352683521021, -0.5064333040532968);


CREATE TABLE mesures_physiques.instrumentation_site (
    id serial,
    id_materiel integer NOT NULL,
    id_site integer NOT NULL,
    date_debut date NOT NULL,
    date_fin date NOT NULL,
    PRIMARY KEY (id)
);
INSERT INTO mesures_physiques.instrumentation_site (id_materiel, id_site, date_debut, date_fin) VALUES
    (1, 1, '2022-03-21', '2022-04-04'), 
    (1, 4, '2022-04-04', '2022-04-19'),
    (2, 2, '2022-03-22', '2022-04-19'),
    (3, 3, '2022-03-21', '2022-04-19'),
    (4, 5, '2022-03-21', '2022-04-19'),
    (5, 6, '2022-03-31', '2022-04-19') ;

CREATE INDEX ON mesures_physiques.instrumentation_site
    (id_materiel);
CREATE INDEX ON mesures_physiques.instrumentation_site
    (id_site);


CREATE TABLE mesures_physiques.niveau_bruit (
    id bigint NOT NULL,
    date_heure timestamp without time zone NOT NULL,
    id_materiel integer NOT NULL,
    leq_a numeric NOT NULL,
    leq_lin numeric,
    crete_c numeric,
    fast_inst_a numeric,
    fast_max_a numeric,
    fast_min_a numeric,
    PRIMARY KEY (id)
);
CREATE INDEX mesures_physiques_niveau_bruit_date_heure_materiel ON mesures_physiques.niveau_bruit(date_heure, id_materiel) ;

CREATE INDEX ON mesures_physiques.niveau_bruit
    (id_materiel);


CREATE TABLE mesures_physiques.spectre (
    id bigserial,
    id_niveau_bruit bigint NOT NULL,
    type_freq varchar(20) NOT NULL,
    freq numeric NOT NULL,
    valeur numeric NOT NULL,
    PRIMARY KEY (id)
);

CREATE INDEX ON mesures_physiques.spectre
    (id_niveau_bruit);
CREATE INDEX ON mesures_physiques.spectre
    (type_freq);
CREATE INDEX ON mesures_physiques.spectre
    (freq);


CREATE TABLE mesures_physiques.enum_type_freq (
    type_freq varchar(20) NOT NULL,
    PRIMARY KEY (type_freq)
);
INSERT INTO mesures_physiques.enum_type_freq (type_freq) VALUES
    ('octave'),('tiers octave');



CREATE TABLE mesures_physiques.enum_freq (
    freq numeric NOT NULL,
    PRIMARY KEY (freq)
);
INSERT INTO mesures_physiques.enum_freq (freq) VALUES
    (6), (8), (10), (12), (16), (20), (25), (31), (40), (50), (63), (80), (100), (125), (160), (200), (250),
    (315), (400), (500), (630), (800), (1000), (1250), (1600), (2000), (2500), (3150), (4000), (5000), (6300), (8000), (10000), (12500), (16000), (20000);


CREATE TABLE mesures_physiques.meteo (
    id int NOT NULL,
    date_heure timestamp without time zone NOT NULL,
    id_materiel integer NOT NULL,
    vts_vent_haut numeric NOT NULL,
    vit_vent_bas NUMERIC NOT NULL,
    temp_haut NUMERIC NOT NULL,
    temp_bas NUMERIC NOT NULL,
    dir_vent_haut NUMERIC NOT NULL,
    dir_vent_bas numeric,
    hygro_haut numeric,
    hygro_bas numeric,
    rayonnement numeric,
    pluie numeric,
    PRIMARY KEY (id)
);


ALTER TABLE mesures_physiques.materiel_metrologique ADD CONSTRAINT FK_materiel_metrologique__nature FOREIGN KEY (nature) REFERENCES mesures_physiques.enum_nature_materiel(nature) ON UPDATE CASCADE;
ALTER TABLE mesures_physiques.materiel_metrologique ADD CONSTRAINT FK_materiel_metrologique__classe FOREIGN KEY (classe) REFERENCES mesures_physiques.enum_classe_materiel(classe) ON UPDATE CASCADE;
ALTER TABLE mesures_physiques.materiel_metrologique ADD CONSTRAINT FK_materiel_metrologique__marque FOREIGN KEY (marque) REFERENCES mesures_physiques.enum_marque(marque) ON UPDATE CASCADE;
ALTER TABLE mesures_physiques.materiel_metrologique ADD CONSTRAINT FK_materiel_metrologique__modele FOREIGN KEY (modele) REFERENCES mesures_physiques.enum_modele(modele) ON UPDATE CASCADE;
ALTER TABLE mesures_physiques.instrumentation_site ADD CONSTRAINT FK_instrumentation_site__id_materiel FOREIGN KEY (id_materiel) REFERENCES mesures_physiques.materiel_metrologique(id);
ALTER TABLE mesures_physiques.instrumentation_site ADD CONSTRAINT FK_instrumentation_site__id_site FOREIGN KEY (id_site) REFERENCES mesures_physiques.site_mesure(id);
ALTER TABLE mesures_physiques.niveau_bruit ADD CONSTRAINT FK_niveau_bruit__id_materiel FOREIGN KEY (id_materiel) REFERENCES mesures_physiques.materiel_metrologique(id);
ALTER TABLE mesures_physiques.spectre ADD CONSTRAINT FK_spectre__id_niveau_bruit FOREIGN KEY (id_niveau_bruit) REFERENCES mesures_physiques.niveau_bruit(id);
ALTER TABLE mesures_physiques.spectre ADD CONSTRAINT FK_spectre__type_freq FOREIGN KEY (type_freq) REFERENCES mesures_physiques.enum_type_freq(type_freq) ON UPDATE CASCADE;
ALTER TABLE mesures_physiques.spectre ADD CONSTRAINT FK_spectre__freq FOREIGN KEY (freq) REFERENCES mesures_physiques.enum_freq(freq) ON UPDATE CASCADE;
ALTER TABLE mesures_physiques.meteo ADD CONSTRAINT FK_meteo__id_materiel FOREIGN KEY (id_materiel) REFERENCES mesures_physiques.materiel_metrologique(id) ON UPDATE CASCADE; 