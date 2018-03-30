package sample.model;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class Datasource {

    public static final String DB_NAME = "music.db";
    public static final String CONNECTION_STRING = "jdbc:sqlite:" + DB_NAME;
    public static final String TABLE_ALBUMS = "albums";
    public static final String COLUMN_ALBUM_ID= "_id";
    public static final String COLUMN_ALBUM_NAME = "name";
    public static final String COLUMN_ALBUM_ARTIST = "artist";
    public static final int INDEX_ALBUM_ID = 1;
    public static final int INDEX_ALBUM_NAME= 2;
    public static final int INDEX_ALBUM_ARTIST = 3;

    public static final String TABLE_ARTISTS= "artists";
    public static final String COLUMN_ARTIST_ID = "_id";
    public static final String COLUMN_ARTIST_NAME = "name";
    public static final int INDEX_ARTIST_ID = 1;
    public static final int INDEX_ARTIST_NAME= 2;

    public static final String TABLE_SONGS= "songs";
    public static final String COLUMN_SONG_ID = "_id";
    public static final String COLUMN_SONG_TRACK = "track";
    public static final String COLUMN_SONG_TITLE = "title";
    public static final String COLUMN_SONG_ALBUM = "album";
    public static final int INDEX_SONG_ID = 1;
    public static final int INDEX_SONG_TRACK= 2;
    public static final int INDEX_SONG_TITLE = 3;
    public static final int INDEX_SONG_ALBUM = 4;

    public static final int ORDER_BY_NONE=1;
    public static final int ORDER_BY_ASC = 2; // It sorts the result set in ascending order by expression
    public static final int ORDER_BY_DESC = 3; //It sorts the result set in descending order by expression.

    public static final String QUERY_ALBUMS_BY_START =
            "SELECT " + TABLE_ALBUMS + '.' + COLUMN_ALBUM_NAME + " FROM " + TABLE_ALBUMS +
            " INNER JOIN " + TABLE_ARTISTS + " ON " + TABLE_ALBUMS + '.' + COLUMN_ALBUM_ARTIST +
            " = " + TABLE_ARTISTS + "." + COLUMN_ARTIST_ID +
            " WHERE " + TABLE_ARTISTS + '.' + COLUMN_ARTIST_NAME + " = \"";

    public static final String QUERY_ALBUMS_BY_ARTIST_SORT =
            " ORDER BY " + TABLE_ALBUMS + '.' + COLUMN_ALBUM_NAME + " COLLATE NOCASE ";

    public static final String QUERY_ARTIST_FOR_SONG_START =
            "SELECT " + TABLE_ARTISTS + '.' + COLUMN_ARTIST_NAME + ", " +
                    TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + "," +
                    TABLE_SONGS + "." + COLUMN_SONG_TRACK + " FROM " + TABLE_SONGS +
                    " INNER JOIN " + TABLE_ALBUMS + " ON " +
                    TABLE_SONGS+ "." + COLUMN_SONG_ALBUM + " = " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ID +
                    " INNER JOIN " + TABLE_ARTISTS + " ON " +
                    TABLE_ALBUMS + "." + COLUMN_ALBUM_ARTIST + " = " + TABLE_ARTISTS + "." + COLUMN_ARTIST_ID +
                    " WHERE " + TABLE_SONGS + "." + COLUMN_SONG_TITLE + " = \"";

    public static final String QUERY_ARTIST_FOR_SONG_SORT =
            "ORDER BY " + TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + ", " +
                    TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + " COLLATE NOCASE ";

    public static final String TABLE_ARTIST_SONG_VIEW = "artist_list";
    public static final String CREATE_ARTIST_FOR_SONG_VIEW = "CREATE VIEW IF NOT EXISTS " +
            TABLE_ARTIST_SONG_VIEW + " AS SELECT " + TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + ", " +
            TABLE_ALBUMS + "." + COLUMN_ALBUM_NAME + " AS " + COLUMN_SONG_ALBUM + ", " +
            TABLE_SONGS + "." + COLUMN_SONG_TRACK + ", " + TABLE_SONGS + "." + COLUMN_SONG_TITLE +
            " FROM " + TABLE_SONGS +
            " INNER JOIN " + TABLE_ALBUMS + " ON " + TABLE_SONGS +
            "." + COLUMN_SONG_ALBUM + " = " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ID +
            " INNER JOIN " + TABLE_ARTISTS + " ON " + TABLE_ALBUMS + "." + COLUMN_ALBUM_ARTIST +
            " ORDER BY " +
            TABLE_ARTISTS + "." + COLUMN_ARTIST_NAME + ", " +
            TABLE_ALBUMS+ "." + COLUMN_ALBUM_NAME + ", " +
            TABLE_SONGS + "." + COLUMN_SONG_TRACK;

    public static final String QUERY_VIEW_SONG_INFO_PREP = "SELECT " + COLUMN_ARTIST_NAME +
            ", " + COLUMN_SONG_TRACK + " FROM " + TABLE_ARTIST_SONG_VIEW +
            " WHERE " + COLUMN_SONG_TITLE + " = ?";


    public static final String INSERT_ARTIST = "INSERT INTO " + TABLE_ARTISTS +
            '(' + COLUMN_ARTIST_NAME + ") VALUES(?)";

    public static final String INSERT_ALBUMS = "INSERT INTO " + TABLE_ALBUMS +
            '(' + COLUMN_ALBUM_NAME + ", " + COLUMN_ALBUM_ARTIST + ") VALUES(?, ?)";

    public static final String INSERT_SONGS = "INSERT INTO " + TABLE_SONGS +
            '(' + COLUMN_SONG_TRACK + ", " + COLUMN_SONG_TITLE + ", " + COLUMN_SONG_ALBUM +
            ") VALUES(?, ?, ?)";

    public static final String QUERY_ARTIST = "SELECT " + COLUMN_ARTIST_ID + " FROM " +
            TABLE_ARTISTS + " WHERE " + COLUMN_ARTIST_NAME + " =?";

    public static final String QUERY_ALBUM = "SELECT " + COLUMN_ALBUM_ID + " FROM " +
            TABLE_ALBUMS + " WHERE " + COLUMN_ALBUM_NAME + " =?";

    public static final String QUERY_ALBUMS_BY_ARTIST_ID = "SELECT * FROM " + TABLE_ALBUMS +
            " WHERE " + COLUMN_ALBUM_ARTIST + " = ? ORDER BY " + COLUMN_ALBUM_NAME + " COLLATE NOCASE";


    private Connection connection;

    private PreparedStatement querySongInfoView;

    private PreparedStatement insertIntoArtists;
    private PreparedStatement insertIntoAlbums;
    private PreparedStatement insertIntoSongs;

    private PreparedStatement queryArtist;
    private PreparedStatement queryAlbum;
    private PreparedStatement queryAlbumsByArtistId;

    private static Datasource instance = new Datasource();
    private Datasource(){

    }

    public static Datasource getInstance(){
        return instance;
    }

    public boolean open(){
        try{
            connection = DriverManager.getConnection(CONNECTION_STRING);
            querySongInfoView = connection.prepareStatement(QUERY_VIEW_SONG_INFO_PREP);
            insertIntoArtists = connection.prepareStatement(INSERT_ARTIST, Statement.RETURN_GENERATED_KEYS);
            insertIntoAlbums = connection.prepareStatement(INSERT_ALBUMS, Statement.RETURN_GENERATED_KEYS);
            insertIntoSongs = connection.prepareStatement(INSERT_SONGS);
            queryArtist = connection.prepareStatement(QUERY_ARTIST);
            queryAlbum = connection.prepareStatement(QUERY_ALBUM);
            queryAlbumsByArtistId = connection.prepareStatement(QUERY_ALBUMS_BY_ARTIST_ID);

            return true;
        }catch (SQLException e){
            System.out.println("Couldnt connect to database: " + e.getMessage());
            return false;
        }
    }

    public void close(){
        try{

            if(querySongInfoView != null){
                querySongInfoView.close();
            }

            if(insertIntoArtists !=null){
                insertIntoArtists.close();
            }

            if(insertIntoAlbums !=null){
                insertIntoAlbums.close();
            }

            if(insertIntoSongs !=null){
                insertIntoSongs.close();
            }

            if(queryArtist !=null){
                queryArtist.close();
            }

            if(queryAlbum !=null){
                queryAlbum.close();
            }

            if(queryAlbumsByArtistId !=null){
                queryAlbumsByArtistId.close();
            }

            if(connection != null){
                connection.close();
            }
        }catch (SQLException e){
            System.out.println("Couldnt connect to database: " + e.getMessage());
        }
    }

    public List<Artist> queryArtist(int sortOrder){

        StringBuilder sb = new StringBuilder("SELECT * FROM ");
        sb.append(TABLE_ARTISTS);
        if(sortOrder != ORDER_BY_NONE){
            sb.append(" ORDER BY ");
            sb.append(COLUMN_ARTIST_NAME);
            sb.append(" COLLATE NOCASE ");
            if(sortOrder == ORDER_BY_DESC) {
                sb.append("DESC");
            }else {
                sb.append("ASC");
            }
        }

        try(Statement statement = connection.createStatement();
           ResultSet result = statement.executeQuery(sb.toString())){

            List<Artist> artists = new ArrayList<>();
            while(result.next()){
                Artist artist = new Artist();
                artist.setId(result.getInt(INDEX_ARTIST_ID));
                artist.setName(result.getString(INDEX_ARTIST_NAME));
                artists.add(artist);
            }

            return artists;

        }catch (SQLException e){
            System.out.println("Query failed: " + e.getMessage());
            return null;
        }

    }

    public List<Album> queryAlbumForArtistId(int id){
        try{
            queryAlbumsByArtistId.setInt(1, id);
            ResultSet resultSet = queryAlbumsByArtistId.executeQuery();

            List<Album> albums = new ArrayList<>();
            while(resultSet.next()){
                Album album = new Album();
                album.setId(resultSet.getInt(1));
                album.setName(resultSet.getString(2));
                album.setArtistid(id);
                albums.add(album);
            }
            return albums;
        }catch (SQLException e ){
            System.out.println("QUERY FAILED: " + e.getMessage());
            return null;
        }
    }

    public List<String> queryAlbumsForArtist(String artistName, int sortOrder){
        StringBuilder sb = new StringBuilder(QUERY_ALBUMS_BY_START);
        sb.append(artistName);
        sb.append("\"");

        if(sortOrder != ORDER_BY_NONE){
            sb.append(QUERY_ALBUMS_BY_ARTIST_SORT);
            if (sortOrder == ORDER_BY_DESC) {
                sb.append("DESC");
            } else {
                sb.append("ASC");
            }
        }
        System.out.println("SQL statement = " + sb.toString());
        try(Statement statement = connection.createStatement();
            ResultSet result = statement.executeQuery(sb.toString())) {

            List<String> albums = new ArrayList<>();
            while(result.next()){
                albums.add(result.getString(1));
            }

            return albums;

        }catch (SQLException e){
            System.out.println("Query failed: " + e.getMessage());
            return null;
        }
        }

        public void querySongsMetadata() {
            String sql = "SELECT * FROM " + TABLE_SONGS;

            try (Statement statement = connection.createStatement();
                 ResultSet result = statement.executeQuery(sql)) {

                ResultSetMetaData meta = result.getMetaData(); // getting the column's name, types and attributes etc.
                int numColumns = meta.getColumnCount();
                for (int i = 1;  i<= numColumns; i++) { //printing each column name
                    System.out.format("Column %d in the songs table is name %s\n",
                            i,meta.getColumnName(i));
                }
            } catch (SQLException e) {
                System.out.println("Query failed: " + e.getMessage());
            }

        }

        public int getCount(String table){
        String sql = "SELECT COUNT (*) AS count FROM " + table; //assigning the return value to count
            try (Statement statement = connection.createStatement();
                 ResultSet result = statement.executeQuery(sql)) {

                int count = result.getInt("count");

                System.out.format("Count = %d\n", count);
                return count;
            }catch (SQLException e ){
                System.out.println("Query failed: " + e.getMessage());
                return -1;
            }
    }

    public boolean createViewForSongArtist(){
        try (Statement statement = connection.createStatement()){
             statement.execute(CREATE_ARTIST_FOR_SONG_VIEW);
             return true;

        }catch (SQLException e ){
            System.out.println("Create view failed: " + e.getMessage());
            return false;
        }
    }

        private int insertArtist(String name) throws SQLException{
        queryArtist.setString(1,name);
        ResultSet resultSet = queryArtist.executeQuery();
        if(resultSet.next()){
            //if queryArtist exists
            return resultSet.getInt(1); //always at column 1
        }else{
            //Insert the artist if doesn't exist
            insertIntoArtists.setString(1,name);
            int affectedRows = insertIntoArtists.executeUpdate(); // executeUpdate returns the numbers of row affected

            if(affectedRows != 1){
                throw  new SQLException("Couldn't insert artist!");
            }
            ResultSet generatedKeys = insertIntoArtists.getGeneratedKeys();
            if(generatedKeys.next()){
                return generatedKeys.getInt(1);
            }else {
                throw new SQLException("Couldn't get _id for artist!");
            }
         }
        }

    private int insertAlbum(String name, int artistId) throws SQLException{

        queryAlbum.setString(1,name);
        ResultSet resultSet = queryAlbum.executeQuery();
        if(resultSet.next()){
            //if queryArtist exists
            return resultSet.getInt(1); //always at column 1
        }else{
            //Insert the album if doesn't exist
            insertIntoAlbums.setString(1,name);
            insertIntoAlbums.setInt(2,artistId);
            int affectedRows = insertIntoAlbums.executeUpdate(); // executeUpdate returns the numbers of row affected

            if(affectedRows != 1){
                throw  new SQLException("Couldn't insert album!");
            }
            ResultSet generatedKeys = insertIntoAlbums.getGeneratedKeys();
            if(generatedKeys.next()){
                return generatedKeys.getInt(1);
            }else {
                throw new SQLException("Couldn't get _id for album!");
            }
        }
    }

    public void insertSong(String title, String artist, String album, int track) {
        try{
        connection.setAutoCommit(false);

        int artistId = insertArtist(artist);
        int albumId = insertAlbum(album, artistId);
        insertIntoSongs.setInt(1, track);
        insertIntoSongs.setString(2, title);
        insertIntoSongs.setInt(3, albumId);
                int affectedRows = insertIntoSongs.executeUpdate(); // executeUpdate returns the numbers of rows affected
                if(affectedRows == 1){
                    //and if there was only one row affected we commit the changes and ends the transaction
                    connection.commit();
                }else {
                    throw new SQLException("Th song insert failed!");
                }
        }catch (Exception e){
            System.out.println("Insert song exception: " + e.getMessage());
            try{
                System.out.println("Performing rollback");
                connection.rollback(); //back out any transaction
            }catch (SQLException e2) {
                System.out.println("Oh boy! You messed up!");
            }
            }finally {
                try{
                    System.out.println("Resetting default commit!");
                    connection.setAutoCommit(true);
                }catch (SQLException e){
                    System.out.println("Couldn't reset auto-commit! " + e.getMessage());
                }
            }
    }
}

