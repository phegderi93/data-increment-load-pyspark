def create_bronze_table(spark):
    spark.sql("""
        create table if not exists bronze_db.bronze_customer_v1 (
            customer_id string,
            name string,
            address string,
            city string,
            state string,
            zip_code string,
            phone_number string,
            email string,
            date_of_birth string,
            plan_type string,
            audit_created_timestamp string,
            year string,
            month string,
            day string

        )
    USING PARQUET

    PARTITIONED BY (
    audit_created_timestamp
    )
    LOCATION '/Users/prasad.hegde/Desktop/LearningProject/data-warehouse-Learning/tables/bronze/customer/v1'
        ;
        """)

    spark.sql("""
        create table if not exists bronze_db.bronze_product_v1 (
                product_id string,
                product_name string,
                brand string,
                type string,
                flavor string,
                size string,
                price string,
                expiration_dt string,
                image_url string,
                audit_created_timestamp string,
                year string,
                month string,
                day string

                    )
                USING PARQUET

                PARTITIONED BY (
                audit_created_timestamp
                )
                LOCATION '/Users/prasad.hegde/Desktop/LearningProject/data-warehouse-Learning/tables/bronze/product/v1'
                    ;
                    """)

    spark.sql("""
        create table if not exists bronze_db.bronze_store_v1 (
                store_id string,
                store_name string,
                address string,
                city string,
                state string,
                zip_code string,
                phone_number string,
                audit_created_timestamp string,
                year string,
                month string,
                day string

                    )
                USING PARQUET

                PARTITIONED BY (
                audit_created_timestamp
                )
                LOCATION '/Users/prasad.hegde/Desktop/LearningProject/data-warehouse-Learning/tables/bronze/store/v1'
                    ;
                    """)

    spark.sql("""
            create table if not exists bronze_db.bronze_product_v1 (
                product_id string,
                product_name string,
                brand string,
                type string,
                flavor string,
                size string,
                price double,
                expiration_dt string,
                image_url string,
                audit_created_timestamp string,
                year string,
                month string,
                day string
            )
            USING PARQUET

                PARTITIONED BY (
                audit_created_timestamp
                )
                LOCATION '/Users/prasad.hegde/Desktop/LearningProject/data-warehouse-Learning/tables/bronze/product/v1'
            ;
            """)

    spark.sql("msck repair table bronze_db.bronze_customer_v1")
    spark.sql("msck repair table bronze_db.bronze_product_v1")
    spark.sql("msck repair table bronze_db.bronze_store_v1")
    spark.sql("msck repair table bronze_db.bronze_product_v1")


def bronze_to_silver_view(spark):
    spark.sql("""
              create or replace view bronze_db.bronze_customer_v1_bronze_to_silver_view as
              select 
              customer_id ,
            split(name, " ")[0] as first_name,
            split(name, " ")[1] as last_name,
            address ,
            city ,
            state ,
            zip_code ,
            phone_number ,
            email ,
            TO_DATE(date_of_birth, 'dd-MM-yyyy') AS date_of_birth,
            plan_type ,
            audit_created_timestamp,
            cast(current_timestamp() as string) as audit_effective_from_timestamp,
            cast('1999-12-31 00:00:00' as string) as audit_effective_to_timestamp,
            year,
            month,
            day
            from bronze_db.bronze_customer_v1
            ;
            """)

    spark.sql("""
              create or replace view bronze_db.bronze_product_v1_bronze_to_silver_view as
              select 
               product_id,
                product_name ,
                brand ,
                type ,
                flavor ,
                size ,
                cast(price AS double) as price,
                cast(expiration_dt AS date) as expiration_dt,
                image_url,
                audit_created_timestamp,
                cast(current_timestamp() as string) as audit_effective_from_timestamp,
                cast('1999-12-31 00:00:00' as string) as audit_effective_to_timestamp,
                year,
                month
            from bronze_db.bronze_product_v1
            ;
            """)


def create_silver_table(spark):
    spark.sql(
        """
        create table if not exists silver_db.silver_customer  (
                customer_id string,
                first_name string,
                last_name string,
                address string,
                city string,
                state string,
                zip_code string,
                phone_number string,
                email string,
                date_of_birth date,
                plan_type string,
                audit_created_timestamp string,
                audit_effective_from_timestamp string,
                audit_effective_to_timestamp string,
                year string,
                month string,
                day string
            )
           USING DELTA        
            LOCATION '/Users/prasad.hegde/Desktop/LearningProject/data-warehouse-Learning/tables/silver/customer'
            ;
            """)

    spark.sql("""
            create table if not exists silver_db.silver_product (
                product_id string,
                product_name string,
                brand string,
                type string,
                flavor string,
                size string,
                price double,
                expiration_dt date,
                image_url string,
                audit_created_timestamp string,
                audit_effective_from_timestamp timestamp,
                audit_effective_to_timestamp timestamp,
                year string,
                month string,
                day string
            )
            USING delta
            LOCATION '/Users/prasad.hegde/Desktop/LearningProject/data-warehouse-Learning/tables/silver/product'
            ;
            """
              )

    spark.sql("""
                create table if not exists silver_db.silver_store (
                    store_id string,
                    store_name string,
                    address string,
                    city string,
                    state string,
                    zip_code string,
                    phone_number string,
                    audit_created_timestamp string,
                    audit_effective_from_timestamp timestamp,
                    audit_effective_to_timestamp timestamp,
                    year string,
                    month string,
                    day string
                )
                USING delta
                LOCATION '/Users/prasad.hegde/Desktop/LearningProject/data-warehouse-Learning/tables/silver/store'
                ;
                                """)

    spark.sql("""
                create table if not exists silver_db.silver_product (
                        product_id string,
                        product_name string,
                        brand string,
                        type string,
                        flavor string,
                        size string,
                        price double,
                        expiration_dt date,
                        image_url string,
                        effective_start_dt timestamp,
                        effective_end_dt timestamp,
                        active_flg int,
                        audit_created_timestamp string,
                        audit_effective_from_timestamp timestamp,
                        audit_effective_to_timestamp timestamp,
                        year string,
                        month string,
                        day string
                )
                USING delta
                LOCATION '/Users/prasad.hegde/Desktop/LearningProject/data-warehouse-Learning/tables/silver/product'
                ;
                                """)

    spark.sql(
        """
        create table if not exists silver_db.control_table (
        table_name string,
        last_updated_timestamp date
        )
        USING delta
        LOCATION '/Users/prasad.hegde/Desktop/LearningProject/data-warehouse-Learning/tables/silver/control'
        ;
                        """)

    # spark.sql("insert into silver_db.control_table (table_name, last_updated_timestamp) values ('silver_customer', '2025-07-19')")


