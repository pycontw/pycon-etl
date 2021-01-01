dir_2020_postevent_data="${HOME}/pycontw-report-generator-data"

csv_talks="${dir_2020_postevent_data}/misc/talks-list.csv"
csv_talks_proposed="${dir_2020_postevent_data}/misc/talks-list-proposed.csv"
csv_booth="${dir_2020_postevent_data}/misc/booth.csv"

csv_attendee_individual="${dir_2020_postevent_data}/tickets/with-identities/individual-attendees.csv"
csv_attendee_reserved="${dir_2020_postevent_data}/tickets/with-identities/reserved-attendees.csv"
csv_attendee_corporate="${dir_2020_postevent_data}/tickets/with-identities/corporate-attendees.csv"

yaml="${dir_2020_postevent_data}/yamls/general-plots-en.yaml"
yaml_package="${dir_2020_postevent_data}/yamls/packages.yaml"
yaml_sponsor="${dir_2020_postevent_data}/yamls/sponsors.yaml"


rg-cli \
--talks-csv  ${csv_talks} \
--proposed-talks-csv ${csv_talks_proposed} \
--booth-csv ${csv_booth} \
--csv  ${csv_attendee_individual} \
--csv  ${csv_attendee_reserved} \
--csv  ${csv_attendee_corporate} \
--yaml ${yaml} \
--package-yaml ${yaml_package} \
--sponsor-yaml ${yaml_sponsor}
