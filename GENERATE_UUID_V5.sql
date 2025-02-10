CREATE FUNCTION dataset.GENERATE_UUID_V5(reference_var STRING) RETURNS STRING AS (
    -- full query wrapped in parens thanks to this BigQuery error message:
    -- "The body of each CREATE FUNCTION statement is an expression, not a query;
    -- to use a query as an expression, the query must be wrapped with additional
    -- parentheses to make it a scalar subquery expression"
    (
        WITH generated_uuid AS (
            SELECT
                TO_HEX(
                    CAST(
                        TO_HEX(
                            LEFT(
                                SHA1(
                                    CONCAT(
                                        -- namespace in bytes: OID
                                        b'k\xa7\xb8\x12\x9d\xad\x11\xd1\x80\xb4\x00\xc0O\xd40\xc8',
                                        CAST(reference_var AS BYTES FORMAT 'UTF8')
                                    )
                                ), 16)
                        ) AS BYTES FORMAT 'HEX'
                    )
                    & CAST('ffffffffffff0fff3fffffffffffffff' AS BYTES FORMAT 'HEX')
                    | CAST('00000000000050008000000000000000' AS BYTES FORMAT 'HEX')
              ) AS uuid
        )
        SELECT CONCAT(
            SUBSTR(uuid,1,8), "-",
            SUBSTR(uuid,9,4), "-",
            SUBSTR(uuid,13,4), "-",
            SUBSTR(uuid,17,4), "-",
            SUBSTR(uuid,21,12)
            )
        FROM generated_uuid
    )
);

SELECT 
    reference_var,
    dataset.GENERATE_UUID_V5(reference_var) AS uuid
FROM UNNEST(['123','123456']) AS reference_var;
