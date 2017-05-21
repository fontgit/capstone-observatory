//import observatory.Location
//val stations = "010013,,,\n724017,03707,+37.358,-078.438\n724017,,+37.350,-078.433"
//val temps = "010013,,11,25,39.2\n724017,,08,11,81.14\n724017,03707,12,06,32\n724017,03707,01,29,35.6"
//stations.split('\n')
//
//temps.split('\n')
//"010013,,11,25,39.2".split(',')
//def farToCel(far: Double): Double = {
//  (far-32.0)*5/9
//}
//farToCel(39.2)
//def stationToLocation (station: String): Location = {
//
//}
val streamStations: InputStream = getClass.getResourceAsStream("stations.csv")

Source.fromInputStream(streamStations).getLines
