select '{{ts}}'
        , '{{ts}}' at time zone 'America/Santiago' + interval '30 min'
        , '{{ts_nodash_with_tz}}' at time zone 'America/Santiago' + interval '30 min';
