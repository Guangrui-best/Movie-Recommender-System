
public class Driver {
    public static void main(String[] args) throws Exception {
        MovieListCollect movieListCollect = new MovieListCollect();
        SetupRating setupRating = new SetupRating();
        DataDividerByUser dataDividerByUser = new DataDividerByUser();
        CoOccurrenceMatrixGenerator coOccurrenceMatrixGenerator = new CoOccurrenceMatrixGenerator();
        Normalize normalize = new Normalize();
        Multiplication multiplication = new Multiplication();
        Sum sum = new Sum();

        String rawInput = args[0];
        String userMovieListOutputDir = args[1];
        String coOccurrenceMatrixDir = args[2];
        String normalizeDir = args[3];
        String multiplicationDir = args[4];
        String sumDir = args[5];

        String movieListDir = args[6];
        String newRatingDir = args[7];

        String[] path1 = { rawInput, userMovieListOutputDir };
        String[] path2 = { userMovieListOutputDir, coOccurrenceMatrixDir };
        String[] path3 = { coOccurrenceMatrixDir, normalizeDir };
        String[] path4 = { normalizeDir, newRatingDir, multiplicationDir };
        String[] path5 = { multiplicationDir, sumDir };
        String[] path6 = { rawInput, movieListDir };
        String[] path7 = { rawInput, newRatingDir, movieListDir };

        movieListCollect.main(path6);
        setupRating.main(path7);
        dataDividerByUser.main(path1);
        coOccurrenceMatrixGenerator.main(path2);
        normalize.main(path3);
        multiplication.main(path4);
        sum.main(path5);

    }

}
