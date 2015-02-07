/**
 * 
 */
package code.dws.utils;

import gnu.trove.set.hash.THashSet;

import java.util.concurrent.Callable;

import org.apache.commons.lang3.tuple.ImmutablePair;

import code.dws.dto.PairDto;

import com.google.common.collect.Sets;

/**
 * @author arnab
 */
public class WorkerOV implements Callable<PairDto> {

	private String originalArg1;
	private String originalArg2;

	private THashSet<ImmutablePair<String, String>> revbSubObj1;
	private THashSet<ImmutablePair<String, String>> revbSubObj2;

	public WorkerOV(String arg1, String arg2,
			THashSet<ImmutablePair<String, String>> revbSubObj1,
			THashSet<ImmutablePair<String, String>> revbSubObj2) {
		this.originalArg1 = arg1;
		this.originalArg2 = arg2;
		this.revbSubObj1 = revbSubObj1;
		this.revbSubObj2 = revbSubObj2;

	}

	public WorkerOV(THashSet<ImmutablePair<String, String>> revbSubObj1,
			THashSet<ImmutablePair<String, String>> revbSubObj2) {

	}

	@Override
	public PairDto call() throws Exception {
		double score = getInstanceOverlapSimilarityScores(revbSubObj1,
				revbSubObj2);

		return new PairDto(this.originalArg1, this.originalArg2, score);
	}

	/**
	 * method to compute properties sharing reverb instances
	 * 
	 * @param revbSubObj1
	 * @param revbSubObj2
	 * 
	 * @param id2
	 * @param id
	 * @param writerOverlap
	 * @return
	 * @return
	 * @throws Exception
	 */
	private static double getInstanceOverlapSimilarityScores(
			THashSet<ImmutablePair<String, String>> revbSubObj1,
			THashSet<ImmutablePair<String, String>> revbSubObj2)
			throws Exception {

		long min = Math.min(revbSubObj1.size(), revbSubObj2.size());

		double scoreOverlap = 0;

		// scoreOverlap = (double) CollectionUtils.intersection(revbSubObj1,
		// revbSubObj2)
		// .size() / min;

		scoreOverlap = (double) Sets.intersection(revbSubObj1, revbSubObj2)
				.size() / min;

		return scoreOverlap;

	}
}
