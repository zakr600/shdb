#include <iostream>

#include "db.h"
#include "interpreter.h"

void cmd()
{
    auto db = shdb::connect("./mydb", 1024);
    auto interpreter = shdb::Interpreter(db);

    while (!std::cin.eof())
    {
        std::cout << "shdb> " << std::flush;
        std::string line;
        std::getline(std::cin, line);
        if (line.empty())
            continue;

        try
        {
            auto row_set = interpreter.execute(line);
            for (const auto & row : row_set.getRows())
                std::cout << row << std::endl;
        }
        catch (const std::exception & ex)
        {
            std::cout << "Error: " << ex.what() << std::endl;
        }
    }
}

int main(int argc, char ** argv)
{
    (void)(argc);
    (void)(argv);
    cmd();
    return 0;
}
