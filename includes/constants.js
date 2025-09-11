const SCHEMAS = {
    prd: {
        raw: "raw",
        stg: "stg",
        mart: "mart"
    },
    dev: {
        raw: "raw_dev",
        stg: "stg_dev",
        mart: "mart_dev"
    }
}

function S(env = "prd") {
    return SCHEMAS[env] || SCHEMAS.prd;
}
module.exports = {
    S
}
