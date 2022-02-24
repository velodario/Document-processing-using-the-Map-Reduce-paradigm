

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Tema2 {
    private static String out;
    static List<String> files_testing = new ArrayList<>();
    static int DIM_FRAGMENT, NR_DOC, WORKERS;


    void write_to_file(List<List<String>> data) {

        try {
            FileWriter myWriter = new FileWriter(out);
            for(List a : data) {
                String list = Arrays.toString(a.toArray()).replace("[", "")
                        .replace("]", "").replaceAll("\\s+","");
                myWriter.write(list+"\n");
            }

            myWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



     Hashtable<String, Hashtable<Integer,Integer>> CallMap(String location,ExecutorService exec,
                        Future<Hashtable<String, Hashtable<Integer,Integer>>> res)
            throws IOException, ExecutionException, InterruptedException {
         Hashtable<String, Hashtable<Integer,Integer>> output;

        Scanner file_scan = new Scanner(new File(location)).useDelimiter(",\\s*\\n");
        List<String> tokens = new ArrayList<>();
        while (file_scan.hasNext()) {
            tokens.add(file_scan.nextLine());
        }
        file_scan.close();

        String[] tempsArray = tokens.toArray(new String[0]);

        DIM_FRAGMENT = Integer.parseInt(tempsArray[0]);
        NR_DOC = Integer.parseInt(tempsArray[1]);
        //fisierele de input
         files_testing.addAll(Arrays.asList(tempsArray).subList(2, tempsArray.length));
         for (String doc : files_testing) {
             //trimitem la Map numele fisierului, offset initial, D
             res = exec.submit(new Map(doc, 0, DIM_FRAGMENT));
         }
         //primim rezultatul
        output = res.get();
        exec.shutdown();


        return output;
    }

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        if (args.length < 3) {
            System.err.println("Usage: Tema2 <workers> <in_file> <out_file>");
            return;
        }
        WORKERS = Integer.parseInt(args[0]);
        String in = args[1];
        out = args[2];
      Tema2 tema = new Tema2();

        ExecutorService doing_map = Executors.newFixedThreadPool(WORKERS);
        //future pentru rezultat map
        Future<Hashtable<String, Hashtable<Integer,Integer>>> future = null;
        //Hashtable contine nume fisier ca key si value un alt hashtable cu key lungimea cuvantului si value nr aparitii
        Hashtable<String, Hashtable<Integer,Integer>> fisier_lungimi;
        fisier_lungimi = tema.CallMap(in,doing_map,future);
     //procesul de Map a fost executat

        ExecutorService doing_reduce = Executors.newFixedThreadPool(WORKERS);
        List<List<String>> data;
        //future pentru rezultat reduce
        Future<List<List<String>>> next_future;

       //incepe treaba workerilor
        next_future = doing_reduce.submit(new Reduce(fisier_lungimi,doing_map,future));
        data = next_future.get();

        doing_reduce.shutdown();
        //primim rezultatul de la reduce, scriem in fisierul de output
        tema.write_to_file(data);

    }
}
