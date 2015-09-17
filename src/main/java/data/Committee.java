package data;

public enum Committee {
    board("Ausschuss", "A", "C", "C"),
    supervisoryBoard("Aufsichtsrat", "AufR.", "CS", "CS"),
    advisoryBoard("Beirat", "Bei.", "CC", "CC"),
    bankAdvisory("Bankrat", "Br.", "CB", "CB"),
    headOffice("Direktion", "D", "D", "D"),
    managementBoard("Geschaeftsleitung", "GL", "G.aff", "G.amm."),
    managementDirectorate("Geschaeftsleitung/Vorstand", "GL, V", "D,CD", "D,CD"),
    generalMeeting("Generalversammlung", "GV", "AGe", "AG"),
    patronage("Patronat", "Pat.", "Pat.", "Pat."),
    foundationBoard("Stiftungsrat", "Sr.", "CF", "CF"),
    directorate("Vorstand", "V", "CD", "CD"),
    administrationBoard("Verwaltungsrat", "VR", "CA", "CdA"),
    administration("Verwaltung", "Vw.", "Admin.", "Ammin."),
    centralCommittee("Zentralausschuss", "ZA", "CCe", "CCe"),
    centralDirectorate("Zentralvorstand", "ZV", "CDC", "CDC"),
    undefined("keine Angaben","-","-","-");

    private final String committeeName;
    private final String de;
    private final String fr;
    private final String it;

    Committee(String committeeName, String de, String fr, String it) {
        this.committeeName = committeeName;
        this.de = de;
        this.fr = fr;
        this.it = it;
    }

    public String getCommitteeName() {
        return committeeName;
    }

    public static Committee parse(String abbr) {
        for(Committee c : values()) {
            if(c.de.equals(abbr) || c.fr.equals(abbr) || c.it.equals(abbr)) {
                return c;
            }
        }
        return undefined;
    }
}
