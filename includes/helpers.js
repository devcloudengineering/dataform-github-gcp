// includes/helpers.js
function dateProcess() {
  const d = dataform.projectConfig.vars.fecha_proceso; 
  return `CAST(${JSON.stringify(d)} AS DATE)`;
}
module.exports = { dateProcess };