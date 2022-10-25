select {{ts_with_tz}}
        , {{ts}}
        , {{ts_with_tz}} at time zone 'America/Santiago'
        , {{ts_nodash_with_tz}} at time zone 'America/Santiago';