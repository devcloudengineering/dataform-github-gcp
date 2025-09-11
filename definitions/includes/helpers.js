// includes/helpers.js
function procDate() {
  // Devuelve un pedazo de SQL seguro con la fecha del var
  const d = dataform.projectConfig.vars.proc_date; // p.ej. "2025-09-10"
  return `CAST(${JSON.stringify(d)} AS DATE)`;
}
module.exports = { procDate };