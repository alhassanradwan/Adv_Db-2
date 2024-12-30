import os
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from PIL import Image

# Cassandra setup
client_id = "JBufEDkPjHeJJnhoZfzalTyj"
secret = "_fLXPLy_D-cwD7-UppedWLSLnMww4bTpunameqfi64Gi.detlcDadii7GFs.uh3aQMx,2YR6wzfW4d2_hwj8JNI8QUKna_Bhk-+yS0MEC6HC3.fewCihCsH3bfL1_FFB"
secure_connect_bundle_path = 'C:\\Users\\alhas\\Downloads\\secure-connect-fcc-tutorial.zip'

auth_provider = PlainTextAuthProvider(client_id, secret)
cluster = Cluster(cloud={'secure_connect_bundle': secure_connect_bundle_path}, auth_provider=auth_provider)
session = cluster.connect()

keyspace = 'movies'
session.set_keyspace(keyspace)

# Function to query movies by actor or director
def query_movies_by_person(session, output_folder):
    search_by = input("Would you like to search by actor or director? Choose (a) for actor, (d) for director: ").strip().lower()

    if search_by == 'd':
        name_type = "Directors"
        person_name = input("Enter the director's name: ")
    elif search_by == 'a':
        name_type = "Actors"
        person_name = input("Enter the actor's name: ")
    else:
        print("Invalid choice. Please choose 'a' for actor or 'd' for director.")
        return

    rows = session.execute("SELECT * FROM movie")
    found_movies = False

    for row in rows:
        movie_cast = row.movie_cast
        if name_type == "Directors":
            person_in_cast = movie_cast.get('Directors', '')
        else:
            person_in_cast = movie_cast.get('Actors', '')


        if person_name in person_in_cast:
            found_movies = True
            print(f"Movie Found: Movie ID: {row.id}, Movie Name: {row.name}, Movie Cast: {row.movie_cast}")

            if row.movie_poster:
                poster_path = os.path.join(output_folder, f"{row.name.replace(' ', '_')}.jpg")
                with open(poster_path, "wb") as file:
                    file.write(row.movie_poster)
                print(f"Poster saved to: {poster_path}")
                image = Image.open(poster_path)
                image.show()

    if not found_movies:
        print(f"No movies found for {name_type} '{person_name}'.")



# Function to convert an image to a blob
def image_to_blob(image_path):
    with open(image_path, 'rb') as file:
        return file.read()

# Function to update the movie poster
def update_movie_poster(session, movie_id, image_path):
    try:
        poster_blob = image_to_blob(image_path)
        query = "UPDATE movie SET movie_poster = %s WHERE id = %s"
        session.execute(query, (poster_blob, movie_id))
        print(f"Poster for movie ID {movie_id} has been updated.")
    except Exception as e:
        print(f"Failed to update poster for movie ID {movie_id}: {e}")


# Main logic to search movies or update posters
def main():
    # Define the output folder for saving movie posters
    output_folder = "C:\\Users\\alhas\\Downloads\\movie_posters"
    os.makedirs(output_folder, exist_ok=True)  # Ensure the folder exists

    while True:
        print("\nOptions:")
        print("1. Query movies by actor/director")
        print("2. Update movie posters")
        print("3. Exit")

        choice = input("Choose an option: ").strip()

        if choice == '1':
            # Query movies by actor or director
            query_movies_by_person(session, output_folder)
        elif choice == '2':
            # Update movie posters
            movies = [
                {"id": 1, "path": "C:\\Users\\alhas\\Downloads\\the_matrix_.jpg"},
                {"id": 2, "path": "C:\\Users\\alhas\\Downloads\\the_inception_.jpg"},
                {"id": 3, "path": "C:\\Users\\alhas\\Downloads\\the_dark_knight_.jpg"}
            ]
            for movie in movies:
                update_movie_poster(session, movie["id"], movie["path"])
                print(f"Poster for movie ID {movie['id']} updated successfully.")
        elif choice == '3':
            print("Exiting...")
            break
        else:
            print("Invalid choice. Please try again.")


if __name__ == "__main__":
    main()
