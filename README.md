# tse_bi_loader


# Table *tse_import*

### Table Description

+-----------------+---------------------+----------+---------------------+---------+--------+-----------+
| text            | epoch               | tags     | epoch_end           | patient | record | annotator |
+-----------------+---------------------+----------+---------------------+---------+--------+-----------+
| R_PL 2021:02:08 | 2020-12-17 16:43:16 | ["seiz"] | 2020-12-17 16:44:31 | PAT_6   | 79     | R_PL      |
| R_PL 2021:02:08 | 2020-12-17 18:21:59 | ["seiz"] | 2020-12-17 18:22:32 | PAT_6   | 79     | R_PL      |
| R_PL 2021:02:08 | 2020-12-17 19:12:20 | ["seiz"] | 2020-12-17 19:14:05 | PAT_6   | 79     | R_PL      |
| R_PL 2021:02:08 | 2020-12-17 21:12:25 | ["seiz"] | 2020-12-17 21:13:32 | PAT_6   | 79     | R_PL      |
| R_PL 2021:02:08 | 2020-12-17 21:56:22 | ["seiz"] | 2020-12-17 21:57:48 | PAT_6   | 85     | R_PL      |
| R_PL 2021:02:08 | 2020-12-17 23:08:19 | ["seiz"] | 2020-12-17 23:09:18 | PAT_6   | 85     | R_PL      |
| R_PL 2021:02:08 | 2020-12-18 00:39:32 | ["seiz"] | 2020-12-18 00:40:49 | PAT_6   | 85     | R_PL      |
| R_PL 2021:02:08 | 2020-12-18 05:49:15 | ["seiz"] | 2020-12-18 05:49:53 | PAT_6   | 85     | R_PL      |
+-----------------+---------------------+----------+---------------------+---------+--------+-----------+

### Creation of table

CREATE TABLE tse_import (
    text varchar(25),
    tags varchar(25),
    epoch TIMESTAMP,
    epoch_end TIMESTAMP,
    patient tinytext,
    record int(4),
    annotator tinytext,
    CONSTRAINT pk_tse_import PRIMARY KEY (text(25), patient(5), record, epoch)
); 

### Use

ex:
./sql_send_tse_file.sh -i /home/aura-alexis/github/csv_to_tse_bi/temp_tse/

### Associated request in Grafana

SELECT
    UNIX_TIMESTAMP(epoch) + 3600 as time,
    UNIX_TIMESTAMP(epoch_end) + 3600 as timeend,
    text as text,
    tags as tags
  FROM tse_import
  WHERE record = $record_selection AND patient = '$patient_selection'
  ORDER BY epoch ASC
  LIMIT 100