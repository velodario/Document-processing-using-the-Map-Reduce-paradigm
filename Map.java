
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Hashtable;
import java.util.concurrent.Callable;

public class Map implements Callable<Hashtable<String, Hashtable<Integer,Integer>>> {
    String nume_fis;
    long offset;
    long len = 0;
    String delimitator = ";|:|/|\\?|~|\\.|,|>|<|`|\\[|]|\\{|}|\\(|\\)|!|@|#|\\$|%|\\^|&|-|_|\\+|'|=|\\*|\"|\\|| |\t|\r|\n";
    static Hashtable<String, Hashtable<Integer,Integer>> lungime_rep = new Hashtable<>();

    public Map(String nume_fis, long offset, int D) {
        this.nume_fis = nume_fis;
        this.offset = offset;
    }


    @Override
    public Hashtable<String, Hashtable<Integer,Integer>> call() throws Exception {
        File f = new File(nume_fis);
        //buffer cu dimensiune lungimea fisierului
        byte[] buffer = new byte[(int) f.length()];
       // System.out.println((int) f.length());

        FileInputStream is = null;
        try {
            is = new FileInputStream(nume_fis);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        //pentru fiecare worker prelucram inputul
        //nu am folosit dimensiunea deoarece in anumite situatii
        //ar fi trebuit sa depasesc limita dimensiunei (de ex urmatorul caracter nu e delimitator)
        while (offset <= len) {
            //atata timp cat octetul e letter sau numar, il procesam, altfel e delimitator, ne oprim
            if (!Character.isLetterOrDigit(buffer[(int) offset])) {
                break;
            }
            offset++;
        }

        try {
            //procesam lungimea cuvantului
            offset = is.read(buffer);
        } catch (IOException e) {
            e.printStackTrace();
        }

        //update from byte to String
        String s = new String(buffer);
        //in caracterelor neanalizate precum spatiu
        String[] rez = s.split(delimitator);
        Hashtable<Integer,Integer> lg = new Hashtable<>();
        int repet;
        for (String sc : rez) {
            //gasim lungimea cuvintelor
            int lungime = sc.length();
            //atata timp cat cuvantul nu are lungime 0
            if (lungime > 0) {
                if (lungime_rep.containsKey(nume_fis)) { //daca fisierul deja exista in hashtable
                    if(lg.containsKey(lungime)) { //daca lungimea deja prezenta
                        repet = lg.get(lungime);
                        repet++; //incrementam aparitia
                    } else {
                        repet = 1; //altfel e prima data cand apare
                    }
                } else {
                    if(lg.containsKey(lungime)) {
                        repet = lg.get(lungime);
                        repet++;
                    }
                        else {
                            repet = 1;
                        }
                }
                //hashtable cu lungimea cuvantului si nr aparitii
                 lg.put(lungime, repet);
                //hashtable cu numele fisierului si hashtable mentionata mai sus
                lungime_rep.put(nume_fis,lg);
            }

        }


        return lungime_rep;
    }
}

