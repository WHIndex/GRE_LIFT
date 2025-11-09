#include"./src/src/core/lift.h"
#include"../indexInterface.h"

template<class KEY_TYPE, class PAYLOAD_TYPE>
class LIFTOLInterface : public indexInterface<KEY_TYPE, PAYLOAD_TYPE> {
public:
    void init(Param *param = nullptr) {}

    void bulk_load(std::pair <KEY_TYPE, PAYLOAD_TYPE> *key_value, size_t num, Param *param = nullptr);

    bool get(KEY_TYPE key, PAYLOAD_TYPE &val, Param *param = nullptr);

    bool put(KEY_TYPE key, PAYLOAD_TYPE value, Param *param = nullptr);

    bool update(KEY_TYPE key, PAYLOAD_TYPE value, Param *param = nullptr);

    bool remove(KEY_TYPE key, Param *param = nullptr);

    size_t scan(KEY_TYPE key_low_bound, size_t key_num, std::pair <KEY_TYPE, PAYLOAD_TYPE> *result,
                Param *param = nullptr);

    long long memory_consumption() { return liftol.total_size(); }

private:
    liftol::LIFT <KEY_TYPE, PAYLOAD_TYPE> liftol;
};

template<class KEY_TYPE, class PAYLOAD_TYPE>
void LIFTOLInterface<KEY_TYPE, PAYLOAD_TYPE>::bulk_load(std::pair <KEY_TYPE, PAYLOAD_TYPE> *key_value, size_t num,
                                                      Param *param) {
    liftol.bulk_load(key_value, static_cast<int>(num));
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool LIFTOLInterface<KEY_TYPE, PAYLOAD_TYPE>::get(KEY_TYPE key, PAYLOAD_TYPE &val, Param *param) {
    return liftol.at(key, &val);
    // return false;
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool LIFTOLInterface<KEY_TYPE, PAYLOAD_TYPE>::put(KEY_TYPE key, PAYLOAD_TYPE value, Param *param) {
    liftol.insert(key, value);
    return true;
    // return false;
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool LIFTOLInterface<KEY_TYPE, PAYLOAD_TYPE>::update(KEY_TYPE key, PAYLOAD_TYPE value, Param *param) {
    // return liftol.update(key, value);
    return false;
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
bool LIFTOLInterface<KEY_TYPE, PAYLOAD_TYPE>::remove(KEY_TYPE key, Param *param) {
    // return liftol.remove(key);
    return false;
}

template<class KEY_TYPE, class PAYLOAD_TYPE>
size_t LIFTOLInterface<KEY_TYPE, PAYLOAD_TYPE>::scan(KEY_TYPE key_low_bound, size_t key_num,
                                                   std::pair <KEY_TYPE, PAYLOAD_TYPE> *result,
                                                   Param *param) {
    auto scan_size = liftol.range_scan_by_size(key_low_bound, static_cast<uint32_t>(key_num), result);
    return scan_size;
}