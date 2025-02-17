Checkstyle for the Embulk project
==================================

* google_check.xml: Downloaded from: https://github.com/checkstyle/checkstyle/blob/checkstyle-9.3/src/main/resources/google_checks.xml
     * Commit: 5c1903792f8432243cc8ae5cd79a03a004d3c09c
* checkstyle.xml: Customized from google_check.xml.
    * To enable suppressions through checkstyle-suppressions.xml.
    * To enable suppressions with @SuppressWarnings.
    * To indent with 4-column spaces.
    * To limit columns to 180 characters.
    * To reject unused imports.
