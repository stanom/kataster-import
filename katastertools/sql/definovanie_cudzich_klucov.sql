-- Vytvorenie cudzích kľúčov medzi tabuľkami schém 'kataster' <-> 'ciselnik'

-- #############
-- ### kn_cs ###
ALTER TABLE kataster.kn_cs ADD FOREIGN KEY (don) REFERENCES ciselnik.don(id) ;
ALTER TABLE kataster.kn_cs ADD FOREIGN KEY (drs) REFERENCES ciselnik.drs(id) ;
-- ALTER TABLE kataster.kn_cs ADD FOREIGN KEY (pkk) REFERENCES ciselnik.pkk(id) ; -- ZNEAKTIVNENE, lebo: 'Key columns "pkk" and "id" are of incompatible types: character varying and numeric.'
ALTER TABLE kataster.kn_cs ADD FOREIGN KEY (ums) REFERENCES ciselnik.ums(id) ;


-- #############
-- ### kn_ep ###
ALTER TABLE kataster.kn_ep ADD FOREIGN KEY (don) REFERENCES ciselnik.don(id) ;
ALTER TABLE kataster.kn_ep ADD FOREIGN KEY (drp) REFERENCES ciselnik.drp(id) ;
-- ALTER TABLE kataster.kn_ep ADD FOREIGN KEY (drv) REFERENCES ciselnik.drv(id) ; -- ZNEAKTIVNENE, lebo niektoré hodnoty nespárované
-- ALTER TABLE kataster.kn_ep ADD FOREIGN KEY (pkk) REFERENCES ciselnik.pkk(id) ; -- ZNEAKTIVNENE, lebo niektoré hodnoty nespárované
ALTER TABLE kataster.kn_ep ADD FOREIGN KEY (ump) REFERENCES ciselnik.ump(id) ;


-- #############
-- ### kn_pa ###
-- ALTER TABLE kataster.kn_pa ADD FOREIGN KEY (don) REFERENCES ciselnik.don(id) ; -- ZNEAKTIVNENE, lebo niektoré hodnoty nespárované
ALTER TABLE kataster.kn_pa ADD FOREIGN KEY (drp) REFERENCES ciselnik.drp(id) ;
ALTER TABLE kataster.kn_pa ADD FOREIGN KEY (drv) REFERENCES ciselnik.drv(id) ;
ALTER TABLE kataster.kn_pa ADD FOREIGN KEY (pkk) REFERENCES ciselnik.pkk(id) ;
ALTER TABLE kataster.kn_pa ADD FOREIGN KEY (ump) REFERENCES ciselnik.ump(id) ;


-- #############
-- ### kn_pk ###
ALTER TABLE kataster.kn_pk ADD FOREIGN KEY (ump) REFERENCES ciselnik.ump(id) ;


-- #############
-- ### kn_vl ###
ALTER TABLE kataster.kn_vl ADD FOREIGN KEY (tuc) REFERENCES ciselnik.tuc(id) ;
ALTER TABLE kataster.kn_vl ADD FOREIGN KEY (tvl) REFERENCES ciselnik.tvl(id) ;
