WITH partner_feature_flags_day AS (
    SELECT
        acc.id AS account_id,
        acc.name,
        acc.created_at AS acc_created_at,
        ff.flag_name,
        ff.is_enabled_default,
        ff.created_at AS ff_created_at,
        dts.day
    FROM
        {{ source('datasource', 'accounts') }} AS acc
        CROSS JOIN {{ source('datasource', 'feature_flags') }} AS ff
        CROSS JOIN {{ source('datasource', 'dates') }} AS dts
),
final_table AS (
    SELECT
        p.day,
        p.account_id,
        p.name,
        p.flag_name,
        CASE
            WHEN p.day < DATE(p.acc_created_at) THEN NULL
            WHEN p.day < DATE(p.ff_created_at) THEN NULL
            WHEN (TIMESTAMP(p.acc_created_at) < TIMESTAMP(p.ff_created_at) AND p.day < DATE(p.ff_created_at)) THEN NULL
            ELSE COALESCE(afs.is_enabled, p.is_enabled_default)
        END AS is_enabled,
        CASE
            WHEN p.day < DATE(p.acc_created_at) THEN NULL
            WHEN p.day < DATE(p.ff_created_at) THEN NULL
            WHEN (TIMESTAMP(p.acc_created_at) < TIMESTAMP(p.ff_created_at) AND p.day < DATE(p.ff_created_at)) THEN NULL
            ELSE COALESCE(valid_from, GREATEST(ff_created_at, acc_created_at))
        END AS last_updated,
        CASE
            WHEN p.day < DATE(p.acc_created_at) THEN NULL
            WHEN p.day < DATE(p.ff_created_at) THEN NULL
            WHEN (TIMESTAMP(p.acc_created_at) < TIMESTAMP(p.ff_created_at) AND p.day < DATE(p.ff_created_at)) THEN NULL
            WHEN (afs.is_enabled is null and p.is_enabled_default = false) THEN NULL
            WHEN (afs.is_enabled is null and p.is_enabled_default = true) THEN 'activated'
            WHEN afs.is_enabled = true THEN 'activated'
            WHEN afs.is_enabled = false THEN 'deactivated'
            ELSE NULL
        END AS status,
    FROM
        partner_feature_flags_day AS p
        LEFT JOIN {{ source('datasource', 'account_flag_settings') }} AS afs ON p.account_id = afs.account_id
            AND p.flag_name = afs.flag_name
            AND (p.day BETWEEN DATE(afs.valid_from) AND DATE(afs.valid_to) OR (p.day >= DATE(afs.valid_from) AND afs.valid_to IS NULL))
)
SELECT * FROM final_table
ORDER BY day DESC, name, flag_name, last_updated