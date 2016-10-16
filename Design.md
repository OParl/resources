## Das Design von OParl

Dieses Dokument soll die Designentscheidungen hinter der Spezifikation erklären.

### Datum und Uhrzeit

Daten und Zeitstempel basieren auf der ISO 8601, die der internantionale
Standard für Zeitdarstellung ist. Es wurde dabei bewusst ein strengeres Format
als die sehr offene ISO-Norm gewählt, um Inkompatibilitäten zwischen den
verschiedenen Implementierungen zu verhindern und um eine leichtere
Validierbarkeit (z.B. durch einen einfachen Regex) zu ermöglichen.
