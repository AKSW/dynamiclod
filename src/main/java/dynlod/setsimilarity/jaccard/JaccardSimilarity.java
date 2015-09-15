package dynlod.setsimilarity.jaccard;

import java.util.HashSet;
import java.util.Set;


public class JaccardSimilarity{

  public double compare(Set<String> s1, Set<String>s2) {

      Set<String> u = new HashSet<String>();
      u.addAll(s1);
      u.addAll(s2);
      
      Set<String> i = new HashSet<String>();
      i.addAll(s1);
      i.retainAll(s2);
      
      System.out.println((double) i.size() / (double) u.size());
      return (double) i.size() / (double) u.size();
  }

}