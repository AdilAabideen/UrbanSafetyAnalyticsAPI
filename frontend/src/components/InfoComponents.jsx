const CRIME_TYPE_DESCRIPTIONS = {
  "Violence and sexual offences":
    "Offences against the person including harassment, common assault, grievous bodily harm, and sexual offences such as rape and sexual assault.",
  "Public order":
    "Offences that cause fear, alarm or distress in a public setting, including threatening or abusive behaviour, riot, and affray.",
  "Anti-social behaviour":
    "Nuisance or disorder that causes distress to individuals or communities, such as noise, intimidation, vandalism, and street drinking.",
  "Criminal damage and arson":
    "Deliberate destruction or damage to property, including graffiti, smashing windows, and intentionally setting fire to buildings or vehicles.",
  "Shoplifting":
    "Theft of goods from a retail establishment while it is open for business, ranging from concealment to organised retail crime.",
  "Other theft":
    "Theft offences not classified elsewhere, including theft from open areas, making off without payment, and other opportunistic stealing.",
  "Vehicle crime":
    "Theft of or from a motor vehicle, as well as vehicle interference and taking a vehicle without the owner's consent.",
  "Burglary":
    "Unlawful entry into a building or dwelling with the intent to steal, cause damage, or commit grievous bodily harm.",
  "Drugs":
    "Offences related to the possession, supply, production, or trafficking of controlled substances.",
  "Other crime":
    "Miscellaneous criminal offences not captured by other categories, including forgery, perjury, and perverting the course of justice.",
  "Robbery":
    "Theft involving the use or threat of force against a person, including mugging, armed robbery, and carjacking.",
  "Theft from the person":
    "Theft directly from an individual such as pickpocketing or bag-snatching, where no force or threat of force is used.",
  "Possession of weapons":
    "Unlawful possession of firearms, knives, or other offensive weapons in a public place or private setting.",
  "Bicycle theft":
    "Theft of a pedal bicycle from any location, including public bike racks, garages, and open streets.",
};

function InfoComponents({
  crimeType,
  reportedBy,
  location,
  lsoaCode,
  lsoaName,
  outcomeCategory,
  context,
  onClose,
}) {
  const crimeDescription = crimeType ? CRIME_TYPE_DESCRIPTIONS[crimeType] : null;

  return (
    <div className="pointer-events-auto flex h-full w-[375px] flex-col gap-3 overflow-y-auto rounded-xl border border-cyan-200/10 bg-[#071316]/85 p-4 shadow-2xl backdrop-blur-md">
      <div className="flex items-center justify-between">
        <h2 className="text-xl font-semibold tracking-wide text-cyan-50">Crime Information</h2>
        <button
          type="button"
          onClick={onClose}
          className="flex h-7 w-7 items-center justify-center rounded-md text-cyan-100/50 transition-colors hover:bg-cyan-100/10 hover:text-cyan-50"
        >
          <svg xmlns="http://www.w3.org/2000/svg" className="h-4 w-4" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <line x1="18" y1="6" x2="6" y2="18" />
            <line x1="6" y1="6" x2="18" y2="18" />
          </svg>
        </button>
      </div>

      <section className="flex flex-col gap-2 rounded-lg bg-cyan-100/5 p-3">
        <h3 className="text-lg font-medium uppercase tracking-wider text-cyan-100/50">
          Type of Crime
        </h3>
        <div className="flex items-center justify-between text-sm text-cyan-50">
          <span className="text-xl font-semibold">{crimeType ?? "—"}</span>
        </div>
        {crimeDescription && (
          <div className="flex items-center gap-2 text-xs text-cyan-100/60">
            {crimeDescription}
          </div>
        )}
      </section>

      <section className="flex flex-col gap-2 rounded-lg bg-cyan-100/5 p-3">
        <h3 className="text-lg font-medium uppercase tracking-wider text-cyan-100/50">
          Crime Location Report
        </h3>
        <InfoField label="Reported By" value={reportedBy} />
        <InfoField label="Location" value={location} />
        <InfoField label="LSOA Code" value={lsoaCode} />
        <InfoField label="LSOA Name" value={lsoaName} />
        <InfoField label="Last Outcome Category" value={outcomeCategory} />
      </section>

      <section className="flex flex-col gap-2 rounded-lg bg-cyan-100/5 p-3">
        <h3 className="text-lg font-medium uppercase tracking-wider text-cyan-100/50">
          Further Information
        </h3>
        <InfoField label="Context" value={context} />
        <button
          type="button"
          className="w-full rounded-md bg-cyan-100/5 px-4 py-2 text-sm font-medium uppercase tracking-wider text-cyan-50 transition-colors hover:bg-cyan-100/10"
        >
          Provide More Information
        </button>
      </section>
    </div>
  );
}

function InfoField({ label, value }) {
  return (
    <div className="flex flex-col items-start text-sm text-cyan-50">
      <span className="text-sm font-medium uppercase tracking-wider text-cyan-100/50">
        {label}:
      </span>
      <span className="text-xl font-semibold">{value ?? "—"}</span>
    </div>
  );
}

export default InfoComponents;
