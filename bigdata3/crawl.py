import requests
import csv
import os

API_KEY = "7b8e185839abba763a733ece89076913"
BASE_URL = "https://api.themoviedb.org/3"
IMG_URL = "https://image.tmdb.org/t/p/w500"

def get_movies(page=1):
    url = f"{BASE_URL}/movie/popular"
    params = {"api_key": API_KEY, "language": "en-US", "page": page}
    r = requests.get(url, params=params)
    return r.json()

def get_movie_detail(movie_id):
    url = f"{BASE_URL}/movie/{movie_id}"
    params = {"api_key": API_KEY, "language": "en-US", "append_to_response": "videos,credits"}
    r = requests.get(url, params=params)
    return r.json()

if __name__ == "__main__":
    os.makedirs("results", exist_ok=True)
    output_file = "results/tmdb_movies.csv"

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Movie name", "Movie rate", "Description", "Creators", "Trailer link", "More Info"])

        total_movies = 0
        for page in range(1, 76):  # ~1500 movies (20 * 75 pages)
            data = get_movies(page)
            results = data.get("results", [])

            print(f"✅ Page {page} -> found {len(results)} movies")

            for m in results:
                movie_id = m["id"]
                title = m.get("title", "N/A")
                rate = m.get("vote_average", "N/A")
                overview = m.get("overview", "N/A")

                detail = get_movie_detail(movie_id)

                # Creators (directors)
                creators = []
                if "credits" in detail:
                    for crew in detail["credits"].get("crew", []):
                        if crew.get("job") == "Director":
                            creators.append(crew.get("name"))
                creators = ", ".join(creators) if creators else "N/A"

                # Trailer link
                trailer_link = "N/A"
                if "videos" in detail:
                    for v in detail["videos"].get("results", []):
                        if v["type"] == "Trailer" and v["site"] == "YouTube":
                            trailer_link = f"https://www.youtube.com/watch?v={v['key']}"
                            break

                # More info link
                more_info = f"https://www.themoviedb.org/movie/{movie_id}"

                writer.writerow([title, rate, overview, creators, trailer_link, more_info])
                total_movies += 1

        print(f"\n🎉 DONE! Collected {total_movies} movies → check {output_file}")
