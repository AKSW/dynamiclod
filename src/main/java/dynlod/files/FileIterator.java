package dynlod.files;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.function.Consumer;

public class FileIterator implements Iterable<String> {


    private BufferedReader r;

    public FileIterator(String file) throws FileNotFoundException {
    	this.r = new BufferedReader(new FileReader(new File(file)));
    }

    public Iterator<String> iterator() {
        return new Iterator<String>() {

            public boolean hasNext() {
                try {
                    r.mark(1);
                    if (r.read() < 0) {
                        return false;
                    }
                    r.reset();
                    return true;
                } catch (IOException e) {
                    return false;
                }
            }

            public String next() {
                try {
                    return r.readLine();
                } catch (IOException e) {
                    return null;
                }
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }

			public void forEachRemaining(Consumer<? super String> action) {
				// TODO Auto-generated method stub
				
			}

        };
    }

	public void forEach(Consumer<? super String> action) {
		// TODO Auto-generated method stub
		
	}

	public Spliterator<String> spliterator() {
		// TODO Auto-generated method stub
		return null;
	}

}