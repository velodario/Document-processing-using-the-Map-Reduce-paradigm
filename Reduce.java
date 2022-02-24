

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class Reduce implements Callable<List<List<String>>> {
    Hashtable<String, Hashtable<Integer,Integer>> fisier_lungimi;
    ExecutorService exec;
    Future<Hashtable<String, Hashtable<Integer,Integer>>> future;

    public Reduce(Hashtable<String, Hashtable<Integer,
            Integer>> fisier_lungimi, ExecutorService exec,
                  Future<Hashtable<String, Hashtable<Integer, Integer>>> future) {
        this.fisier_lungimi = fisier_lungimi;
        this.exec = exec;
        this.future = future;
    }

    @Override
    public List<List<String>> call() throws Exception {
        HashMap<String,Integer> max_length_per_file = new HashMap<>();
        int keys = 0;
        //fisier_lungimi este hashtable din rezultatul map
        for(Hashtable<Integer,Integer> lungimi : fisier_lungimi.values()) {
            //numele fisierului pentru a stie care fisier ii apartine taskul
            Object firstKey = fisier_lungimi.keySet().toArray()[keys];
            String key_to_String = String.valueOf(firstKey);

            keys++;
            Set<Integer> setOfKeys = lungimi.keySet();
            Iterator<Integer> itr = setOfKeys.iterator();
            int maxKey = 0;

            while (itr.hasNext()) {

               //iteram pentru a gasi lungimea maxima
                int key = itr.next();
                if (key > maxKey) {
                    maxKey = key; //lungime max pt fisier
                }

            }
            //hashmap cu numele fisierului si lungimea maxima a cuvantului
            max_length_per_file.put(key_to_String,maxKey);

        }
        //calulam rangul
        List<List<String>> data = new ArrayList<>();
        int key = 0;
        //acest for ne ajuta pentru sub-hashmapul ce contine lungime si nr aparitii
        for(Hashtable<Integer,Integer> lungimi : fisier_lungimi.values()) {
            float rang;
            int n, apariti, sum = 0, sum_ap = 0;
            for (int i = 0; i < lungimi.size(); i++) {
                //set cu lungimile
                Set<Integer> setOfKeys = lungimi.keySet();
                Integer[] get_lungime = setOfKeys.toArray(new Integer[0]);
                //lungimea curenta
                n = get_lungime[i];
                //nr de aparitii pentru lungimea curenta
                apariti = lungimi.get(n);
                int first = 0, second = 1, next = 0;
                // fibonacci simplu
                for (int j = 1; j <= n + 1; j++) {
                    if (j <= 1)
                        next = j;
                    else {
                        next = first + second;
                        first = second;
                        second = next;
                    }

                }
                sum += next * apariti;
                sum_ap += apariti;


            }
            rang = (float) sum / (float) sum_ap;
            String rang_final = String.format("%.2f", rang);
          //numele fisier curent
            Object cheie = fisier_lungimi.keySet().toArray()[key];
            String current_File = String.valueOf(cheie);
            StringBuilder new_fisier = new StringBuilder();
            //eliminam numele directoarelor
            for(int i=current_File.length()-1; i > 0; i--) {
                if(current_File.charAt(i) == '/') {
                    break;
                }
                new_fisier.append(current_File.charAt(i));
            }
            new_fisier.reverse();
            //lista de lista cu numele fisierului, rang, lungime max, nr aparitii
            data.add(Arrays.asList(new_fisier.toString(),rang_final,max_length_per_file.get(current_File).toString(),
                    lungimi.get(max_length_per_file.get(current_File)).toString()));
            key++;
        }

        //sortam descrescator
        for (int i=0; i < data.size(); i++) {
            for (int j=0; j < (data.size()-i-1); j++) {
                if (Double.parseDouble(data.get(j).get(1)) < Double.parseDouble(data.get(j+1).get(1))) {
                    List<String> temp = data.get(j);
                    data.set(j, data.get(j+1));
                    data.set(j+1, temp);
                }
            }
        }

        return data;
    }
}
