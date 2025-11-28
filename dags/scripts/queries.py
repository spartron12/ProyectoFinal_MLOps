
###### DROP ##################################

DROP_TABLE_RAW = """
DROP TABLE IF EXISTS house_raw;
"""

DROP_TABLE_TEMP_RAW = """
DROP TABLE IF EXISTS house_temp_raw;
"""

DROP_TABLE_CLEAN = """
DROP TABLE IF EXISTS house_clean;
"""   

##### TRUNCATE ##################################

TRUNCATE_TABLE_TEMP = """
    TRUNCATE TABLE IF EXISTS house_clean_temp;

"""    

TRUNCATE_TABLE_TEMP_RAW = """
TRUNCATE TABLE house_temp_raw;
"""


#### CREATE

CREATE_TABLE_TEMP_RAW= """ CREATE TABLE IF NOT EXISTS house_temp_raw (
            group_number INT NULL,
            day VARCHAR(50) NULL,
            batch_number INT NULL,
            brokered_by FLOAT NULL,
            status VARCHAR(50) NULL,
            price FLOAT NULL,
            bed FLOAT NULL,
            bath FLOAT NULL,
            acre_lot FLOAT NULL,
            street FLOAT NULL,
            city VARCHAR(50) NULL,
            state VARCHAR(50) NULL,
            zip_code FLOAT NULL,
            house_size FLOAT NULL,
            prev_sold_date VARCHAR(50) NULL
        )
        """


CREATE_TABLE_RAW = """ CREATE TABLE IF NOT EXISTS house_raw (
            group_number INT NULL,
            day VARCHAR(50) NULL,
            batch_number INT NULL,
            brokered_by FLOAT NULL,
            status VARCHAR(50) NULL,
            price FLOAT NULL,
            bed FLOAT NULL,
            bath FLOAT NULL,
            acre_lot FLOAT NULL,
            street FLOAT NULL,
            city VARCHAR(50) NULL,
            state VARCHAR(50) NULL,
            zip_code FLOAT NULL,
            house_size FLOAT NULL,
            prev_sold_date VARCHAR(50) NULL
        )
        """
        

CREATE_TABLE_CLEAN = """ CREATE TABLE IF NOT EXISTS house_clean (
            price FLOAT NULL,
            bed FLOAT NULL,
            bath FLOAT NULL,
            acre_lot FLOAT NULL,
            house_size FLOAT NULL,
            state_Alabama INT NULL,
            state_Alaska INT NULL,
            state_Arizona INT NULL,
            state_Arkansas INT NULL,
            state_California INT NULL,
            state_Colorado INT NULL,
            state_Connecticut INT NULL,
            state_Delaware INT NULL,
            state_District_of_Columbia INT NULL,
            state_Florida INT NULL,
            state_Georgia INT NULL,
            state_Hawaii INT NULL,
            state_Idaho INT NULL,
            state_Illinois INT NULL,
            state_Indiana INT NULL,
            state_Iowa INT NULL,
            state_Kansas INT NULL,
            state_Kentucky INT NULL,
            state_Louisiana INT NULL,
            state_Maine INT NULL,
            state_Maryland INT NULL,
            state_Michigan INT NULL,
            state_Minnesota INT NULL,
            state_Mississippi INT NULL,
            state_Missouri INT NULL,
            state_Montana INT NULL,
            state_Nebraska INT NULL,
            state_Nevada INT NULL,
            state_New_Hampshire INT NULL,
            state_New_Jersey INT NULL,
            state_New_Mexico INT NULL,
            state_New_York INT NULL,
            state_North_Carolina INT NULL,
            state_North_Dakota INT NULL,
            state_Ohio INT NULL,
            state_Oklahoma INT NULL,
            state_Oregon INT NULL,
            state_Pennsylvania INT NULL,
            state_Rhode_Island INT NULL,
            state_South_Carolina INT NULL,
            state_South_Dakota INT NULL,
            state_Tennessee INT NULL,
            state_Texas INT NULL,
            state_Utah INT NULL,
            state_Vermont INT NULL,
            state_Virginia INT NULL,
            state_Washington INT NULL,
            state_West_Virginia INT NULL,
            state_Wisconsin INT NULL,
            state_Wyoming INT NULL,
            price_per_sqft FLOAT NULL,
            lot_per_sqft FLOAT NULL
        )
        """

